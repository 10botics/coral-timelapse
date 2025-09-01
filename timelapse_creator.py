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

def get_images_from_local_storage_by_domain_camera(domain_name, camera_name):
    """Get images from local storage using Google Drive folder structure"""
    try:
        local_path = os.path.join(LOCAL_IMAGE_FOLDER, domain_name, camera_name)
        logger.info(f"🔍 Searching local storage: {local_path}")
        
        # Check if the domain/camera folder exists locally
        full_path = os.path.join(os.getcwd(), local_path)
        if not os.path.exists(full_path):
            logger.warning(f"📁 Local folder not found: {full_path}")
            logger.info(f"💡 This folder will be created when images are synchronized")
            return []
        
        # Get images from the specific domain/camera folder
        image_paths = get_all_images_from_folder(domain_name, camera_name)
        
        if image_paths:
            logger.info(f"✅ Found {len(image_paths)} images in local storage: {domain_name}/{camera_name}")
        else:
            logger.info(f"📁 No images found in local storage: {domain_name}/{camera_name}")
        
        return image_paths
        
    except Exception as e:
        logger.error(f"❌ Error accessing local storage for {domain_name}/{camera_name}: {str(e)}")
        return []

def get_google_drive_images(service, image_folder_id):
    """Get list of images from Google Drive image folder"""
    try:
        logger.info(f"🔍 Fetching images from Google Drive folder ID: {image_folder_id}")
        
        images = []
        page_token = None
        
        while True:
            try:
                # Use the same query format as the original working script
                query = f"'{image_folder_id}' in parents and (mimeType='image/jpeg' or mimeType='image/png' or mimeType='image/jpg') and trashed=false"
                logger.debug(f"🔍 Google Drive query: {query}")
                
                results = service.files().list(
                    q=query,
                    pageSize=1000,
                    pageToken=page_token,
                    fields="nextPageToken, files(id, name, createdTime, size)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
                
                files = results.get('files', [])
                for file in files:
                    images.append({
                        'id': file['id'],
                        'name': file['name'],
                        'createdTime': file.get('createdTime', ''),
                        'size': int(file.get('size', 0))
                    })
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching Google Drive images: {str(e)}")
                break
        
        logger.info(f"📊 Found {len(images)} images in Google Drive")
        return images
        
    except Exception as e:
        logger.error(f"❌ Failed to get Google Drive images: {str(e)}")
        return []

def get_local_storage_info_by_domain_camera(domain_name, camera_name):
    """Get information about images in local storage for specific domain/camera"""
    try:
        local_image_folder = os.path.join(os.getcwd(), LOCAL_IMAGE_FOLDER, domain_name, camera_name)
        
        if not os.path.exists(local_image_folder):
            logger.warning(f"Local image folder not found: {local_image_folder}")
            return {'count': 0, 'size': 0, 'files': []}
        
        image_extensions = ('.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif')
        local_files = []
        total_size = 0
        
        for file in os.listdir(local_image_folder):
            if file.lower().endswith(image_extensions):
                file_path = os.path.join(local_image_folder, file)
                try:
                    file_size = os.path.getsize(file_path)
                    file_stat = os.stat(file_path)
                    local_files.append({
                        'name': file,
                        'path': file_path,
                        'size': file_size,
                        'modified': file_stat.st_mtime
                    })
                    total_size += file_size
                except OSError:
                    logger.warning(f"Could not access file: {file}")
        
        # Sort by modification time (oldest first)
        local_files.sort(key=lambda x: x['modified'])
        
        logger.info(f"📁 Local storage ({domain_name}/{camera_name}): {len(local_files)} images, {total_size / (1024*1024):.2f} MB")
        return {
            'count': len(local_files),
            'size': total_size,
            'files': local_files
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting local storage info for {domain_name}/{camera_name}: {str(e)}")
        return {'count': 0, 'size': 0, 'files': []}

def identify_new_images(google_drive_images, local_storage_info):
    """Identify images that exist in Google Drive but not in local storage"""
    try:
        local_file_names = {os.path.splitext(f['name'])[0] for f in local_storage_info['files']}
        new_images = []
        
        for gd_image in google_drive_images:
            gd_name = os.path.splitext(gd_image['name'])[0]
            if gd_name not in local_file_names:
                new_images.append(gd_image)
        
        logger.info(f"🆕 Found {len(new_images)} new images to download")
        return new_images
        
    except Exception as e:
        logger.error(f"❌ Error identifying new images: {str(e)}")
        return []

def download_new_images(service, new_images, local_image_folder):
    """Download new images from Google Drive to local storage"""
    try:
        if not new_images:
            logger.info("📥 No new images to download")
            return 0
        
        logger.info(f"📥 Downloading {len(new_images)} new images...")
        downloaded_count = 0
        
        for idx, image in enumerate(new_images, 1):
            try:
                logger.info(f"📥 Downloading {idx}/{len(new_images)}: {image['name']}")
                
                # Download the file
                request = service.files().get_media(fileId=image['id'])
                file_path = os.path.join(local_image_folder, image['name'])
                
                with open(file_path, 'wb') as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            logger.debug(f"Downloaded {int(status.progress() * 100)}% of {image['name']}")
                
                downloaded_count += 1
                logger.info(f"✅ Downloaded: {image['name']}")
                
            except Exception as e:
                logger.error(f"❌ Failed to download {image['name']}: {str(e)}")
                continue
        
        logger.info(f"📥 Download complete: {downloaded_count}/{len(new_images)} images downloaded")
        return downloaded_count
        
    except Exception as e:
        logger.error(f"❌ Error downloading new images: {str(e)}")
        return 0

def cleanup_old_images(local_storage_info, max_images):
    """Remove old images if total count exceeds maximum allowed"""
    try:
        current_count = local_storage_info['count']
        
        if current_count <= max_images:
            logger.info(f"📁 Local storage has {current_count} images, within limit of {max_images}")
            return 0
        
        images_to_remove = current_count - max_images
        logger.info(f"🧹 Need to remove {images_to_remove} old images to stay within limit of {max_images}")
        
        # Remove oldest images first (they're sorted by modification time)
        removed_count = 0
        for image in local_storage_info['files'][:images_to_remove]:
            try:
                os.remove(image['path'])
                removed_count += 1
                logger.info(f"🗑️ Removed old image: {image['name']}")
            except OSError as e:
                logger.error(f"❌ Failed to remove {image['name']}: {str(e)}")
        
        logger.info(f"🧹 Cleanup complete: removed {removed_count} old images")
        return removed_count
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {str(e)}")
        return 0

def cleanup_google_drive_overflow(service, image_folder_id, max_images):
    """Remove overflow images from Google Drive to maintain maximum count"""
    try:
        logger.info(f"🧹 Cleaning up Google Drive overflow to maintain {max_images} images...")
        
        # Get all images from Google Drive, sorted by modification time (oldest first)
        query = f"'{image_folder_id}' in parents and (mimeType='image/jpeg' or mimeType='image/png' or mimeType='image/jpg') and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name, createdTime, modifiedTime, size)",
            orderBy="modifiedTime asc",  # Oldest first for overflow removal
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        all_images = results.get('files', [])
        total_images = len(all_images)
        
        if total_images <= max_images:
            logger.info(f"✅ Google Drive has {total_images} images, within limit of {max_images}")
            return 0
        
        # Calculate how many to remove
        images_to_remove = total_images - max_images
        logger.info(f"🧹 Need to remove {images_to_remove} oldest images from Google Drive")
        
        # Remove oldest images first (they're at the beginning of the list due to orderBy="modifiedTime asc")
        removed_count = 0
        for image in all_images[:images_to_remove]:
            try:
                logger.info(f"🗑️ Deleting overflow image: {image['name']} (ID: {image['id']})")
                service.files().delete(fileId=image['id'], supportsAllDrives=True).execute()
                removed_count += 1
                logger.info(f"✅ Deleted: {image['name']}")
            except Exception as delete_error:
                logger.error(f"❌ Failed to delete {image['name']}: {str(delete_error)}")
                continue
        
        logger.info(f"🧹 Google Drive overflow cleanup complete: removed {removed_count} oldest images")
        logger.info(f"📊 Remaining images in Google Drive: {total_images - removed_count}")
        
        return removed_count
        
    except Exception as e:
        logger.error(f"❌ Error during Google Drive overflow cleanup: {str(e)}")
        return 0

def cleanup_overflow_images(service, image_folder_id, domain_name, camera_name, max_images):
    """Comprehensive cleanup of overflow images in both Google Drive and local storage"""
    try:
        logger.info(f"🧹 Starting comprehensive overflow cleanup for {domain_name}/{camera_name}...")
        logger.info(f"📏 Target maximum images: {max_images}")
        
        # Step 1: Check local storage overflow
        local_storage_info = get_local_storage_info_by_domain_camera(domain_name, camera_name)
        local_overflow = max(0, local_storage_info['count'] - max_images)
        
        if local_overflow > 0:
            logger.info(f"📁 Local storage overflow: {local_overflow} images")
            logger.info(f"🧹 Cleaning up local storage overflow...")
            local_removed = cleanup_old_images(local_storage_info, max_images)
            logger.info(f"✅ Local storage cleanup: removed {local_removed} overflow images")
        else:
            logger.info(f"✅ Local storage within limit: {local_storage_info['count']} images")
        
        # Step 2: Check Google Drive overflow
        google_drive_images = get_google_drive_images(service, image_folder_id)
        gd_overflow = max(0, len(google_drive_images) - max_images)
        
        if gd_overflow > 0:
            logger.info(f"☁️ Google Drive overflow: {gd_overflow} images")
            logger.info(f"🧹 Cleaning up Google Drive overflow...")
            gd_removed = cleanup_google_drive_overflow(service, image_folder_id, max_images)
            logger.info(f"✅ Google Drive cleanup: removed {gd_removed} overflow images")
        else:
            logger.info(f"✅ Google Drive within limit: {len(google_drive_images)} images")
        
        # Step 3: Final verification
        final_local = get_local_storage_info_by_domain_camera(domain_name, camera_name)
        final_gd = get_google_drive_images(service, image_folder_id)
        
        logger.info(f"📊 Final cleanup results:")
        logger.info(f"   • Local storage: {final_local['count']} images")
        logger.info(f"   • Google Drive: {len(final_gd)} images")
        logger.info(f"   • Target limit: {max_images} images")
        
        total_removed = local_overflow + gd_overflow
        logger.info(f"✅ Comprehensive overflow cleanup complete: {total_removed} total images removed")
        
        return {
            'local_removed': local_overflow,
            'gd_removed': gd_overflow,
            'total_removed': total_removed
        }
        
    except Exception as e:
        logger.error(f"❌ Error during comprehensive overflow cleanup: {str(e)}")
        return {'local_removed': 0, 'gd_removed': 0, 'total_removed': 0}

def synchronize_images(service, image_folder_id, domain_name, camera_name):
    """Main function to synchronize local storage with Google Drive"""
    try:
        logger.info("🔄 Starting image synchronization process...")
        logger.info(f"🎯 Target: Keep local storage synchronized with Google Drive")
        logger.info(f"📏 Maximum images allowed: {MAX_IMAGES_PER_VIDEO}")
        logger.info(f"📂 Domain/Camera: {domain_name}/{camera_name}")
        
        # Step 1: CAPTURE SNAPSHOT of current state (don't re-count during process)
        logger.info("📊 Step 1: Capturing snapshot of current state...")
        google_drive_images = get_google_drive_images(service, image_folder_id)
        local_storage_info = get_local_storage_info_by_domain_camera(domain_name, camera_name)
        
        # Store the initial counts for consistent decision making
        initial_local_count = local_storage_info['count']
        initial_gd_count = len(google_drive_images)
        
        logger.info(f"📸 Snapshot captured:")
        logger.info(f"   • Local storage: {initial_local_count} images")
        logger.info(f"   • Google Drive: {initial_gd_count} images")
        logger.info(f"   • Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not google_drive_images:
            logger.warning("⚠️ No images found in Google Drive, skipping synchronization")
            return local_storage_info
        
        # Step 2: Identify new images using the snapshot
        logger.info("🔍 Step 2: Identifying new images using snapshot...")
        new_images = identify_new_images(google_drive_images, local_storage_info)
        
        # Step 3: Download new images FIRST (don't delete anything yet)
        if new_images:
            logger.info(f"📥 Step 3: Downloading {len(new_images)} new images FIRST...")
            
            # Create the full domain/camera path for downloads
            local_image_folder = os.path.join(os.getcwd(), LOCAL_IMAGE_FOLDER, domain_name, camera_name)
            
            # Ensure local folder exists (create domain and camera subfolders)
            os.makedirs(local_image_folder, exist_ok=True)
            logger.info(f"📁 Downloading images to: {local_image_folder}")
            
            downloaded_count = download_new_images(service, new_images, local_image_folder)
            
            if downloaded_count > 0:
                logger.info(f"✅ Downloaded {downloaded_count} new images successfully")
            else:
                logger.warning(f"⚠️ No images were downloaded")
        else:
            logger.info("✅ No new images to download")
        
        # Step 4: Calculate total using snapshot + downloads (consistent calculation)
        logger.info("📊 Step 4: Calculating total using snapshot + downloads...")
        total_after_download = initial_local_count + len(new_images)
        
        logger.info(f"📊 Post-download analysis (using snapshot):")
        logger.info(f"   • Initial local count: {initial_local_count}")
        logger.info(f"   • New images downloaded: {len(new_images)}")
        logger.info(f"   • Total after download: {total_after_download}")
        logger.info(f"   • Maximum allowed: {MAX_IMAGES_PER_VIDEO}")
        logger.info(f"   • Overflow: {max(0, total_after_download - MAX_IMAGES_PER_VIDEO)} images")
        
        # Step 5: Clean up local storage overflow using snapshot-based calculation
        if total_after_download > MAX_IMAGES_PER_VIDEO:
            logger.info(f"🧹 Step 5: Cleaning up local storage overflow using snapshot...")
            local_overflow = total_after_download - MAX_IMAGES_PER_VIDEO
            logger.info(f"📁 Need to remove {local_overflow} overflow images from local storage")
            
            # Use the snapshot-based local storage info for cleanup
            removed_count = cleanup_old_images(local_storage_info, MAX_IMAGES_PER_VIDEO)
            logger.info(f"✅ Local storage cleanup: removed {removed_count} overflow images")
        else:
            logger.info(f"✅ Local storage within limit: {total_after_download} images")
            local_overflow = 0
        
        # Step 6: Clean up Google Drive overflow using snapshot (consistent with local logic)
        logger.info("🧹 Step 6: Cleaning up Google Drive overflow using snapshot...")
        total_gd_images = initial_gd_count  # Use snapshot count
        
        logger.info(f"☁️ Google Drive analysis (using snapshot):")
        logger.info(f"   • Total images: {total_gd_images}")
        logger.info(f"   • Maximum allowed: {MAX_IMAGES_PER_VIDEO}")
        logger.info(f"   • Overflow: {max(0, total_gd_images - MAX_IMAGES_PER_VIDEO)} images")
        
        if total_gd_images > MAX_IMAGES_PER_VIDEO:
            gd_overflow = total_gd_images - MAX_IMAGES_PER_VIDEO
            logger.info(f"☁️ Need to remove {gd_overflow} overflow images from Google Drive")
            
            gd_removed = cleanup_google_drive_overflow(service, image_folder_id, MAX_IMAGES_PER_VIDEO)
            logger.info(f"✅ Google Drive cleanup: removed {gd_removed} overflow images")
        else:
            logger.info(f"✅ Google Drive within limit: {total_gd_images} images")
            gd_removed = 0
        
        # Step 7: Final summary using snapshot-based results
        logger.info("📋 Step 7: Synchronization summary (using snapshot)...")
        
        logger.info(f"📊 Final results (based on snapshot):")
        logger.info(f"   • Google Drive: {total_gd_images} images (snapshot)")
        logger.info(f"   • Local storage: {total_after_download} images (calculated)")
        logger.info(f"   • Target limit: {MAX_IMAGES_PER_VIDEO}")
        logger.info(f"🆕 New images downloaded: {len(new_images)}")
        logger.info(f"🧹 Overflow cleanup results:")
        logger.info(f"   • Local storage: {local_overflow} images removed")
        logger.info(f"   • Google Drive: {gd_removed} images removed")
        logger.info(f"   • Total: {local_overflow + gd_removed} images removed")
        
        logger.info("✅ Image synchronization complete!")
        
        # Return updated local storage info for the calling function
        updated_local_info = get_local_storage_info_by_domain_camera(domain_name, camera_name)
        return updated_local_info
        
    except Exception as e:
        logger.error(f"❌ Image synchronization failed: {str(e)}")
        return None

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



def main():
    logger.info("===== Google Drive Timelapse Creator (Domain/Camera Structure Mode) =====")
    try:
        # Check if root folder ID is configured
        if not FOLDER_A_ID:
            logger.error("❌ FOLDER_A_ID not configured in .env file")
            logger.error("Please add FOLDER_A_ID=your_root_folder_id to your .env file")
            return
        
        service = authenticate()
        
        # Verify access to root folder (same as original script)
        if not verify_folder_access(service, FOLDER_A_ID):
            raise ValueError(f"Cannot access root folder with ID: {FOLDER_A_ID}")
        
        hkt = pytz.timezone('Asia/Hong_Kong')
        now = datetime.now(hkt)
        logger.info(f"Current time (HKT): {now}")
        
        logger.info("=== DOMAIN/CAMERA TIMELAPSE CREATION STARTED ===")
        logger.info(f"Root Folder ID: {FOLDER_A_ID}")
        logger.info(f"Local image folder: {LOCAL_IMAGE_FOLDER}")
        logger.info(f"Maximum images per video: {MAX_IMAGES_PER_VIDEO}")
        logger.info(f"Target FPS: 24")
        logger.info(f"Image synchronization: {'Enabled' if ENABLE_IMAGE_SYNC else 'Disabled'}")
        logger.info(f"Sync before video creation: {'Yes' if SYNC_BEFORE_VIDEO else 'No'}")
        logger.info(f"Delete previous file: {'Yes' if DELETE_PREVIOUS_VIDEO else 'No'}")
        logger.info(f"Cleanup unused images: {'Yes' if CLEANUP_UNUSED_IMAGES else 'No'}")
        logger.info(f"Max Google Drive images: {MAX_GOOGLE_DRIVE_IMAGES}")
        logger.info("=" * 50)
        
        # Find all domains and cameras (following Google Drive structure)
        logger.info("🔍 Discovering domains and cameras in Google Drive...")
        
        # Get all location folders (like original script)
        locations = list_subfolders(service, FOLDER_A_ID)
        if not locations:
            logger.error("❌ No location folders found under root folder")
            return
        
        domain_camera_pairs = []
        for location in locations:
            domain_name = location['name']
            logger.info(f"🔍 Checking domain: {domain_name} ({location['id']})")
            
            cameras = list_subfolders(service, location['id'])
            for camera in cameras:
                camera_name = camera['name']
                logger.info(f"  📷 Found camera: {camera_name} ({camera['id']})")
                
                # Check if this domain/camera combination has images locally
                local_images = get_images_from_local_storage_by_domain_camera(domain_name, camera_name)
                
                if local_images:
                    domain_camera_pairs.append({
                        'domain': domain_name,
                        'camera': camera_name,
                        'folder_id': camera['id'],
                        'local_images': local_images,
                        'image_count': len(local_images)
                    })
                    logger.info(f"  ✅ {domain_name}/{camera_name}: {len(local_images)} images available locally")
                else:
                    logger.info(f"  ⏭️ {domain_name}/{camera_name}: No local images (skipping)")
        
        if not domain_camera_pairs:
            logger.error("❌ No domain/camera combinations found with local images")
            logger.error("💡 Make sure images are synchronized to local storage first")
            return
        
        logger.info(f"📊 Found {len(domain_camera_pairs)} domain/camera combinations with local images")
        
        # For now, process the first available domain/camera (maintains current behavior)
        # In the future, this will loop through all combinations
        selected_pair = domain_camera_pairs[0]
        domain_name = selected_pair['domain']
        camera_name = selected_pair['camera']
        camera_folder = {'id': selected_pair['folder_id'], 'name': camera_name}
        
        logger.info(f"🎯 Processing: {domain_name}/{camera_name} ({selected_pair['image_count']} images)")
        
        # Get or create the timelapse folder directly under the camera
        logger.info(f"📁 Looking for timelapse folder in {camera_name}...")
        logger.info(f"🔍 Searching for 'timelapse' folder under {camera_name} (ID: {camera_folder['id']})")
        timelapse_folder_id = find_or_create_folder(service, camera_folder['id'], TIMELAPSE_FOLDER_NAME)
        logger.info(f"✅ Timelapse folder ready: {timelapse_folder_id}")
        logger.info(f"📂 Video will be uploaded to: {domain_name}/{camera_name}/timelapse/ folder")
        
        # Process the selected domain/camera
        logger.info(f"📷 Processing {domain_name}/{camera_name}...")
        
        try:
            # Step 1: Synchronize images between Google Drive and local storage (if enabled)
            if ENABLE_IMAGE_SYNC and SYNC_BEFORE_VIDEO:
                logger.info("🔄 Step 1: Starting image synchronization...")
                
                # Get the image folder ID for the camera
                image_folder_id = find_or_create_folder(service, camera_folder['id'], IMAGE_FOLDER_NAME)
                logger.info(f"📁 {camera_name} image folder ID: {image_folder_id}")
                
                # Synchronize images (download new ones, cleanup old ones)
                sync_result = synchronize_images(service, image_folder_id, domain_name, camera_name)
                
                if sync_result is None:
                    logger.error("❌ Image synchronization failed, cannot proceed with video creation")
                    return
                
                logger.info(f"✅ Image synchronization completed successfully")
                logger.info(f"📊 Local storage now has {sync_result['count']} images")
            else:
                logger.info("⏭️ Image synchronization skipped (disabled or not required)")
                sync_result = None
            
            # Step 2: Get images from local storage (already discovered above)
            logger.info("📁 Step 2: Using images from local storage...")
            image_paths = selected_pair['local_images']
            
            if not image_paths:
                logger.error(f"❌ No images found in local storage for {domain_name}/{camera_name}, cannot create timelapse")
                return
            
            # Limit images to maximum allowed
            if len(image_paths) > MAX_IMAGES_PER_VIDEO:
                logger.info(f"Image count ({len(image_paths)}) exceeds maximum ({MAX_IMAGES_PER_VIDEO})")
                logger.info(f"Limiting to first {MAX_IMAGES_PER_VIDEO} images")
                image_paths = image_paths[:MAX_IMAGES_PER_VIDEO]
            
            logger.info(f"✅ Proceeding with {len(image_paths)} images from {domain_name}/{camera_name} for timelapse creation")
            
            # Store the names of images that will be used in the video (for cleanup later)
            used_image_names = [os.path.basename(image_path) for image_path in image_paths]
            logger.info(f"📋 Images to be used in timelapse: {len(used_image_names)}")
            
            # Create video
            logger.info("🎬 Step 3: Starting video creation process...")
            output_video = os.path.join(TEMP_DIR, f"{camera_name}_timelapse_output.mp4")
            
            if create_video(image_paths, output_video, None):  # Duration not used with fixed FPS
                # Generate video name with timestamp
                video_name = f"timelapse_{now.strftime('%Y%m%d_%H%M%S')}.mp4"
                
                logger.info("🎬 Step 4: Video creation successful, starting upload...")
                logger.info(f"📤 Uploading video to Google Drive folder ID: {timelapse_folder_id}")
                
                try:
                    upload_video(service, timelapse_folder_id, output_video, video_name)
                    logger.info(f"✅ Successfully created and uploaded {camera_name} timelapse video: {video_name}")
                    logger.info(f"📁 Video uploaded to: {domain_name}/{camera_name}/timelapse/ folder in Google Drive")
                    logger.info(f"🎯 Final location: {domain_name}/{camera_name}/timelapse/{video_name}")
                    
                    # Step 5: Comprehensive overflow cleanup (ensure both locations stay within MAX_IMAGES_PER_VIDEO limit)
                    logger.info("🧹 Step 5: Comprehensive overflow cleanup after video creation...")
                    try:
                        image_folder_id = find_or_create_folder(service, camera_folder['id'], IMAGE_FOLDER_NAME)
                        overflow_cleanup_result = cleanup_overflow_images(
                            service, 
                            image_folder_id, 
                            domain_name, 
                            camera_name, 
                            MAX_IMAGES_PER_VIDEO
                        )
                        logger.info(f"✅ Overflow cleanup completed:")
                        logger.info(f"   • Local storage: {overflow_cleanup_result['local_removed']} images removed")
                        logger.info(f"   • Google Drive: {overflow_cleanup_result['gd_removed']} images removed")
                        logger.info(f"   • Total: {overflow_cleanup_result['total_removed']} images removed")
                    except Exception as overflow_error:
                        logger.warning(f"⚠️ Overflow cleanup failed: {str(overflow_error)}")
                        logger.info("🔄 Continuing with final summary...")
                        
                except Exception as upload_error:
                    logger.error(f"❌ Upload failed: {str(upload_error)}")
                    logger.error(f"📁 Video file was created locally at: {output_video}")
                    logger.error(f"💡 You can manually upload this file to Google Drive")
                    logger.error(f"🔍 Upload error details: {type(upload_error).__name__}: {str(upload_error)}")
                    # Don't re-raise - continue with cleanup
            else:
                logger.error(f"❌ Failed to create {camera_name} timelapse video")
                
        except Exception as e:
            logger.error(f"❌ {camera_name} processing failed: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # Final summary
        logger.info("=" * 50)
        logger.info(f"=== {domain_name.upper()}/{camera_name.upper()} TIMELAPSE CREATION COMPLETED ===")
        if ENABLE_IMAGE_SYNC and SYNC_BEFORE_VIDEO and sync_result:
            logger.info(f"📊 Final image count in local storage: {sync_result['count']}")
            logger.info(f"📁 Local storage size: {sync_result['size'] / (1024*1024):.2f} MB")
        logger.info(f"📂 Local storage path: {LOCAL_IMAGE_FOLDER}/{domain_name}/{camera_name}/")
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
