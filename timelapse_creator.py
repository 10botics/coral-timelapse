import os
import io
import subprocess
import logging
from datetime import datetime, timedelta
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError
import tempfile
import shutil
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

def get_storage_base_path():
    """Determine the base storage path based on environment"""
    if os.path.exists('/home/ubuntu/coral-timelapse/persistence_storage'):
        return '/home/ubuntu/coral-timelapse/persistence_storage/photos'  # AWS
    else:
        return os.path.join(os.getcwd(), 'image')  # Local

# Configuration
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Path to your service account key file
SCOPES = [
    'https://www.googleapis.com/auth/drive',           # Full access for reading/sharing
    'https://www.googleapis.com/auth/drive.file'      # Explicit file operations (CRUD) - REQUIRED for delete/add operations
]
FOLDER_A_ID = os.getenv('FOLDER_A_ID')  # Load from .env file

IMAGE_FOLDER_NAME = 'image'
TIMELAPSE_FOLDER_NAME = 'timelapse'
TEMP_DIR = '/home/ubuntu/coral-timelapse/persistence_storage/temp' if os.path.exists('/home/ubuntu/coral-timelapse/persistence_storage') else tempfile.mkdtemp()

# Local image folder path (uses environment detection)
LOCAL_IMAGE_FOLDER = get_storage_base_path()

# Maximum images per timelapse video (24 fps)
#MAX_IMAGES_PER_VIDEO = 21840   # 21,840 images = ~15.2 minutes at 24 FPS
#MAX_IMAGES_PER_VIDEO = 21000   # 21,000 images = ~14.6 minutes at 24 FPS
MAX_IMAGES_PER_VIDEO = 1000     # 1,000 images = ~42 seconds at 24 FPS (for 10-minute total runtime)   

# Image synchronization settings
ENABLE_IMAGE_SYNC = os.getenv('ENABLE_IMAGE_SYNC', 'true').lower() == 'true'
SYNC_BEFORE_VIDEO = os.getenv('SYNC_BEFORE_VIDEO', 'true').lower() == 'true'

# Video management settings
DELETE_PREVIOUS_VIDEO = os.getenv('DELETE_PREVIOUS_VIDEO', 'true').lower() == 'true'

# Google Drive cleanup settings
CLEANUP_UNUSED_IMAGES = os.getenv('CLEANUP_UNUSED_IMAGES', 'true').lower() == 'true'
MAX_GOOGLE_DRIVE_IMAGES = int(os.getenv('MAX_GOOGLE_DRIVE_IMAGES', '100'))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def authenticate():
    """Authenticate with Google Drive API with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Authenticating using service account... (attempt {attempt + 1}/{max_retries})")
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            service = build('drive', 'v3', credentials=credentials)
            
            # Test the connection
            try:
                service.files().list(pageSize=1).execute()
                logger.info("✅ Google Drive API connection successful")
                return service
            except Exception as test_error:
                logger.warning(f"Initial API test failed: {test_error}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logger.info(f"Retrying authentication in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise test_error
                    
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + 1
                logger.warning(f"Authentication attempt {attempt + 1} failed: {str(e)}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Authentication failed after {max_retries} attempts: {str(e)}")
                raise

def verify_folder_access(service, folder_id):
    """Verify folder access with retry logic for network issues"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Verifying access to folder ID: {folder_id} (attempt {attempt + 1}/{max_retries})")
            folder = service.files().get(fileId=folder_id, fields='id, name', supportsAllDrives=True).execute()
            logger.info(f"✅ Folder verified: {folder['name']} (ID: {folder['id']})")
            return True
        except HttpError as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + 1
                logger.warning(f"Folder access attempt {attempt + 1} failed: {str(e)}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"❌ Failed to access folder {folder_id} after {max_retries} attempts: {str(e)}")
                return False
        except Exception as e:
            if "EOF" in str(e) or "SSL" in str(e) or "protocol" in str(e):
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logger.warning(f"Network error (attempt {attempt + 1}): {str(e)}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Network error accessing folder {folder_id} after {max_retries} attempts: {str(e)}")
                    return False
            else:
                logger.error(f"❌ Unexpected error accessing folder {folder_id}: {str(e)}")
                return False

def find_or_create_folder(service, parent_id, folder_name):
    try:
        query = f"'{parent_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        folders = results.get('files', [])
        if folders:
            logger.info(f"Found folder: {folder_name} (ID: {folders[0]['id']})")
            return folders[0]['id']
        
        logger.info(f"Creating folder: {folder_name} under parent ID: {parent_id}")
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(
            body=file_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        logger.info(f"Created folder: {folder_name} (ID: {folder['id']})")
        return folder['id']
    except HttpError as e:
        logger.error(f"Error in find_or_create_folder for {folder_name}: {str(e)}")
        raise

def list_subfolders(service, parent_id):
    """List subfolders with retry logic for network issues"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            query = (
                f"'{parent_id}' in parents and "
                "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            )
            results = service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            folders = results.get('files', [])
            logger.info(f"Found {len(folders)} subfolders under parent ID: {parent_id}")
            return folders
        except HttpError as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + 1
                logger.warning(f"API error listing subfolders (attempt {attempt + 1}): {str(e)}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"❌ Failed to list subfolders under {parent_id} after {max_retries} attempts: {str(e)}")
                return []
        except Exception as e:
            if "EOF" in str(e) or "SSL" in str(e) or "protocol" in str(e):
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logger.warning(f"Network error listing subfolders (attempt {attempt + 1}): {str(e)}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Network error listing subfolders under {parent_id} after {max_retries} attempts: {str(e)}")
                    return []
            else:
                logger.error(f"❌ Unexpected error listing subfolders under {parent_id}: {str(e)}")
                return []

def get_or_create_camera_folders(service, camera_folder_id):
    image_folder_id = find_or_create_folder(service, camera_folder_id, IMAGE_FOLDER_NAME)
    timelapse_folder_id = find_or_create_folder(service, camera_folder_id, TIMELAPSE_FOLDER_NAME)
    return image_folder_id, timelapse_folder_id

def process_camera(service, camera_folder_id, image_folder_id, timelapse_folder_id, now):
    """Process a single camera to create one timelapse video"""
    try:
        # Generate video name with timestamp (removing hourly/daily/weekly prefixes)
        video_name = f"timelapse_{now.strftime('%Y%m%d_%H%M%S')}.mp4"
        
        # Check if video already exists
        if video_exists(service, timelapse_folder_id, video_name):
            logger.info(f"Timelapse video {video_name} already exists, skipping.")
            return

        # Get all images from the local image folder
        logger.info(f"Starting image processing for camera {camera_folder_id}...")
        image_paths = get_all_images_from_folder()
        
        if not image_paths:
            logger.info(f"No images found for camera {camera_folder_id}, skipping.")
            return
        
        # Limit images to maximum allowed
        if len(image_paths) > MAX_IMAGES_PER_VIDEO:
            logger.info(f"Image count ({len(image_paths)}) exceeds maximum ({MAX_IMAGES_PER_VIDEO})")
            logger.info(f"Limiting to first {MAX_IMAGES_PER_VIDEO} images")
            image_paths = image_paths[:MAX_IMAGES_PER_VIDEO]
        
        logger.info(f"Proceeding with {len(image_paths)} images for timelapse creation")
        
        # Create video
        logger.info(f"Starting video creation process for camera {camera_folder_id}...")
        output_video = os.path.join(TEMP_DIR, f"timelapse_output_{camera_folder_id}.mp4")
        
        if create_video(image_paths, output_video, None):  # Duration not used with fixed FPS
            logger.info(f"Video creation successful, starting upload...")
            upload_video(service, timelapse_folder_id, output_video, video_name)
            logger.info(f"✅ Successfully created and uploaded timelapse video: {video_name}")
        else:
            logger.error(f"❌ Failed to create timelapse video: {video_name}")
            
    except Exception as e:
        logger.error(f"Error processing camera {camera_folder_id}: {str(e)}")

def get_all_images_from_folder(domain_name=None, camera_name=None):
    """Get all images from the local image folder following Google Drive structure"""
    try:
        if domain_name and camera_name:
            # Specific domain and camera
            local_image_folder = os.path.join(os.getcwd(), LOCAL_IMAGE_FOLDER, domain_name, camera_name)
            logger.info(f"📁 Looking for images in: {local_image_folder}")
        else:
            # Default to old behavior (backward compatibility)
            local_image_folder = os.path.join(os.getcwd(), LOCAL_IMAGE_FOLDER)
            logger.info(f"📁 Looking for images in: {local_image_folder}")
        
        if not os.path.exists(local_image_folder):
            logger.warning(f"Local image folder not found: {local_image_folder}")
            return []
        
        image_extensions = ('.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif')
        image_paths = []
        
        for file in os.listdir(local_image_folder):
            if file.lower().endswith(image_extensions):
                file_path = os.path.join(local_image_folder, file)
                image_paths.append(file_path)
        
        # Sort by filename for consistent ordering
        image_paths.sort()
        
        logger.info(f"📁 Found {len(image_paths)} images in local folder: {local_image_folder}")
        return image_paths
        
    except Exception as e:
        logger.error(f"❌ Error reading local image folder: {str(e)}")
        return []

def video_exists(service, folder_id, video_name):
    try:
        query = f"'{folder_id}' in parents and name = '{video_name}' and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        return bool(results.get('files', []))
    except HttpError as e:
        logger.error(f"Error checking video existence for {video_name}: {str(e)}")
        return False

def download_images(service, image_folder_id, start_time, end_time, temp_dir):
    try:
        start_utc = start_time.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        end_utc = end_time.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        query = f"'{image_folder_id}' in parents and createdTime >= '{start_utc}' and createdTime < '{end_utc}' and (mimeType='image/jpeg' or mimeType='image/png' or mimeType='image/jpg')"
        results = service.files().list(
            q=query,
            orderBy="createdTime",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        logger.info(f"Found {len(files)} images for period {start_time} to {end_time}")
        image_paths = []
        for idx, file in enumerate(files):
            file_path = os.path.join(temp_dir, f"{idx:04d}.jpg")
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logger.debug(f"Downloaded {int(status.progress() * 100)}% of {file['name']}")
            fh.seek(0)
            with open(file_path, 'wb') as f:
                f.write(fh.read())
            if os.path.getsize(file_path) > 0:
                image_paths.append(file_path)
                logger.info(f"Saved image: {file_path}")
            else:
                logger.warning(f"Empty file downloaded: {file['name']}")
                os.remove(file_path)
        return image_paths
    except HttpError as e:
        logger.error(f"Error downloading images: {str(e)}")
        return []

def create_video(image_paths, output_video, desired_duration):
    if not image_paths:
        logger.warning("No images provided for video creation")
        return False
    try:
        num_images = len(image_paths)
        # Use fixed 24 fps as requested
        fps = 24
        
        # Calculate actual duration based on fixed FPS
        actual_duration = num_images / fps
        
        logger.info(f"Starting video creation process...")
        logger.info(f"Video specifications: {num_images} images at fixed {fps} fps")
        logger.info(f"Expected duration: {actual_duration:.2f} seconds")
        
        # Create a text file listing all images for ffmpeg
        logger.info("Creating image list file for FFmpeg...")
        image_list_file = os.path.join(TEMP_DIR, 'image_list.txt')
        with open(image_list_file, 'w') as f:
            for idx, image_path in enumerate(image_paths, 1):
                f.write(f"file '{image_path}'\n")
                # Log progress every 500 images or at the end
                if idx % 500 == 0 or idx == num_images:
                    progress_percent = (idx / num_images) * 100
                    logger.info(f"Image list creation progress: {idx}/{num_images} ({progress_percent:.1f}%)")
        
        logger.info(f"Image list file created successfully with {num_images} images")
        
        # Start FFmpeg process
        logger.info("Starting FFmpeg video encoding...")
        
        cmd = [
            'ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', image_list_file,
            '-vf', f'fps={fps}',
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-y',  # Overwrite output file if it exists
            output_video
        ]
        
        # Run FFmpeg with real-time output capture
        logger.info("Video encoding in progress...")
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Monitor FFmpeg progress with image processing details
        start_time = time.time()
        last_progress_time = start_time
        processed_frames = 0
        
        logger.info(f"🎬 Starting FFmpeg processing of {num_images} images...")
        
        while True:
            output = process.stderr.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                # Parse FFmpeg output for frame processing information
                if 'frame=' in output:
                    try:
                        # Extract frame number from FFmpeg output
                        frame_info = output.split('frame=')[1].split()[0]
                        if frame_info.isdigit():
                            processed_frames = int(frame_info)
                    except:
                        pass
                
                # Log progress every 5 seconds with actual frame count
                current_time = time.time()
                if current_time - last_progress_time >= 5:
                    if processed_frames > 0:
                        progress_percent = (processed_frames / num_images) * 100
                        remaining_frames = num_images - processed_frames
                        estimated_time_left = (remaining_frames / processed_frames) * (current_time - start_time) if processed_frames > 0 else 0
                        
                        logger.info(f"🎬 FFmpeg Progress: {processed_frames}/{num_images} images processed ({progress_percent:.1f}%) - Elapsed: {current_time - start_time:.1f}s - ETA: {estimated_time_left:.1f}s")
                    else:
                        logger.info(f"🎬 FFmpeg starting up... (elapsed: {current_time - start_time:.1f}s)")
                    last_progress_time = current_time
        
        # Wait for completion and get return code
        return_code = process.poll()
        
        if return_code == 0:
            # Check if output file was created and has size
            if os.path.exists(output_video) and os.path.getsize(output_video) > 0:
                file_size_mb = os.path.getsize(output_video) / (1024 * 1024)
                total_time = time.time() - start_time
                logger.info(f"🎬 FFmpeg processing completed successfully!")
                logger.info(f"📊 Final stats: {num_images} images processed in {total_time:.2f} seconds")
                logger.info(f"📁 Output file: {output_video}")
                logger.info(f"💾 File size: {file_size_mb:.2f} MB")
                logger.info(f"⚡ Processing speed: {file_size_mb/total_time:.2f} MB/s")
                logger.info(f"🎯 Average: {total_time/num_images:.3f} seconds per image")
                return True
            else:
                logger.error("Video file was not created or is empty")
                return False
        else:
            logger.error(f"FFmpeg failed with return code: {return_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error creating video: {str(e)}")
        return False

def upload_video(service, folder_id, video_path, video_name):
    """Upload video with retry logic using the original working method"""
    max_retries = 3
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            logger.info(f"📤 Upload attempt {attempt + 1}/{max_retries} for video: {video_name}")
            
            # Check file exists and get size
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Video file not found: {video_path}")
            
            file_size = os.path.getsize(video_path)
            file_size_mb = file_size / (1024 * 1024)
            logger.info(f"📁 File size: {file_size_mb:.2f} MB")
            
            # Use the original working upload method
            file_metadata = {'name': video_name, 'parents': [folder_id]}
            media = MediaFileUpload(video_path, mimetype='video/mp4')
            
            logger.info(f"🚀 Starting upload to folder ID: {folder_id}")
            logger.info(f"📤 Uploading {file_size_mb:.2f} MB to Google Drive...")
            
            # Execute upload using the original method (this is what was working!)
            response = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"📡 Upload completed, processing response...")
            
            if response and 'id' in response:
                logger.info(f"✅ Upload successful! File ID: {response['id']}")
                logger.info(f"📊 Uploaded size: {file_size_mb:.2f} MB")
                return True
            else:
                raise Exception("Upload completed but no file ID returned")
                
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"⚠️ Upload attempt {attempt + 1} failed: {error_msg}")
            
            # Check if it's a retryable error
            if any(keyword in error_msg.lower() for keyword in ['ssl', 'eof', 'protocol', 'connection', 'timeout']):
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"🔄 Retrying in {delay} seconds... (SSL/Network error)")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"❌ All upload attempts failed due to SSL/Network issues")
                    raise
            else:
                # Non-retryable error
                logger.error(f"❌ Non-retryable upload error: {error_msg}")
                raise
    
    # If we get here, all retries failed
    raise Exception(f"Upload failed after {max_retries} attempts")

def delete_videos_in_folder(service, folder_id):
    try:
        query = f"'{folder_id}' in parents and mimeType='video/mp4' and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        for file in files:
            service.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
            logger.info(f"Deleted video: {file['id']} from folder ID: {folder_id}")
        logger.info(f"Deleted {len(files)} videos from folder ID: {folder_id}")
    except HttpError as e:
        logger.error(f"Error deleting videos from folder ID: {folder_id}: {str(e)}")

def delete_old_images(service, image_folder_id, end_time):
    try:
        end_utc = end_time.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')
        query = f"'{image_folder_id}' in parents and createdTime < '{end_utc}'"
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        for file in files:
            service.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
            logger.info(f"Deleted image: {file['id']}")
        logger.info(f"Deleted {len(files)} images before {end_time}")
    except HttpError as e:
        logger.error(f"Error deleting images: {str(e)}")

def main():
    logger.info("===== Google Drive Timelapse Creator (Single Video Mode) =====")
    try:
        # Check if root folder ID is configured
        if not FOLDER_A_ID:
            logger.error("❌ FOLDER_A_ID not configured in .env file")
            logger.error("Please add FOLDER_A_ID=your_root_folder_id to your .env file")
            return
        
        service = authenticate()
        
        # Verify access to root folder
        if not verify_folder_access(service, FOLDER_A_ID):
            raise ValueError(f"Cannot access root folder with ID: {FOLDER_A_ID}")
        
        hkt = pytz.timezone('Asia/Hong_Kong')
        now = datetime.now(hkt)
        logger.info(f"Current time (HKT): {now}")
        
        logger.info("=== SINGLE VIDEO TIMELAPSE CREATION STARTED ===")
        logger.info(f"Root Folder ID: {FOLDER_A_ID}")
        logger.info(f"Local image folder: {LOCAL_IMAGE_FOLDER}")
        logger.info(f"Maximum images per video: {MAX_IMAGES_PER_VIDEO}")
        logger.info(f"Target FPS: 24")
        logger.info("=" * 50)
        
        # Iterate all Location folders under the root folder
        locations = list_subfolders(service, FOLDER_A_ID)
        if not locations:
            logger.info("No location folders found under the root folder. Nothing to process.")
            return
            
        for location in locations:
            logger.info(f"Processing location folder: {location['name']} ({location['id']})")
            cameras = list_subfolders(service, location['id'])
            if not cameras:
                logger.info(f"No camera folders found under location: {location['name']}")
                continue
                
            for camera in cameras:
                logger.info(f"Processing camera folder: {camera['name']} ({camera['id']})")
                image_folder_id, timelapse_folder_id = get_or_create_camera_folders(service, camera['id'])
                
                process_camera(
                    service,
                    camera['id'],
                    image_folder_id,
                    timelapse_folder_id,
                    now,
                )
                
        logger.info("=" * 50)
        logger.info("=== TIMELAPSE CREATION COMPLETED ===")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Cleaning up temporary directory...")
        shutil.rmtree(TEMP_DIR, ignore_errors=True)

if __name__ == '__main__':
    main()
