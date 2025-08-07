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

# Load environment variables from .env file
load_dotenv()

# Configuration
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Path to your service account key file
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_A_ID = os.getenv('FOLDER_A_ID')  # Load from .env file

IMAGE_FOLDER_NAME = 'image'
TIMELAPSE_FOLDER_NAME = 'timelapse'
TEMP_DIR = tempfile.mkdtemp()

# Image thresholds for each video type
IMAGE_THRESHOLDS = {
    'hourly': 60,
    'daily': 720,
    'weekly': 1440
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def authenticate():
    try:
        logger.info("Authenticating using service account...")
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise

def verify_folder_access(service, folder_id):
    try:
        logger.info(f"Verifying access to folder ID: {folder_id}")
        folder = service.files().get(fileId=folder_id, fields='id, name', supportsAllDrives=True).execute()
        logger.info(f"Folder verified: {folder['name']} (ID: {folder['id']})")
        return True
    except HttpError as e:
        logger.error(f"Failed to access folder {folder_id}: {str(e)}")
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

def get_folder_ids(service):
    if not verify_folder_access(service, FOLDER_A_ID):
        raise ValueError(f"Cannot access folder with ID: {FOLDER_A_ID}")
    folder_b_id = find_or_create_folder(service, FOLDER_A_ID, FOLDER_B_NAME)
    folder_c_id = find_or_create_folder(service, folder_b_id, FOLDER_C_NAME)
    image_folder_id = find_or_create_folder(service, folder_c_id, IMAGE_FOLDER_NAME)
    timelapse_folder_id = find_or_create_folder(service, folder_c_id, TIMELAPSE_FOLDER_NAME)
    
    # Create subfolders for hourly, daily, and weekly videos
    hourly_folder_id = find_or_create_folder(service, timelapse_folder_id, 'hourly')
    daily_folder_id = find_or_create_folder(service, timelapse_folder_id, 'daily')
    weekly_folder_id = find_or_create_folder(service, timelapse_folder_id, 'weekly')
    
    return image_folder_id, hourly_folder_id, daily_folder_id, weekly_folder_id

def get_hourly_video_info(now):
    start_time = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1)
    video_name = f"timelapse_hour_{end_time.strftime('%Y%m%d_%H')}.mp4"
    return start_time, end_time, video_name, 2.5  # 2.5 seconds

def get_daily_video_info(now):
    end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)
    video_name = f"timelapse_day_{end_time.strftime('%Y%m%d')}.mp4"
    return start_time, end_time, video_name, 30  # 30 seconds

def get_weekly_video_info(now):
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    days_to_monday = weekday
    end_time = (now - timedelta(days=days_to_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=7)
    year, week, _ = end_time.isocalendar()
    video_name = f"timelapse_week_{year}{week:02d}.mp4"
    return start_time, end_time, video_name, 60  # 60 seconds

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
        fps = num_images / desired_duration
        logger.info(f"Creating video with {num_images} images at {fps:.2f} fps")
        cmd = [
            'ffmpeg',
            '-framerate', str(fps),
            '-i', os.path.join(os.path.dirname(image_paths[0]), '%04d.jpg'),
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-y',  # Overwrite output file if it exists
            output_video
        ]
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f"ffmpeg output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg failed: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error creating video: {str(e)}")
        return False

def upload_video(service, folder_id, video_path, video_name):
    try:
        file_metadata = {'name': video_name, 'parents': [folder_id]}
        media = MediaFileUpload(video_path, mimetype='video/mp4')
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()
        logger.info(f"Uploaded video: {video_name} to folder ID: {folder_id}")
    except HttpError as e:
        logger.error(f"Error uploading video {video_name}: {str(e)}")
        raise

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
    logger.info("===== Google Drive Timelapse Creator =====")
    
    # Validate environment variables
    if not FOLDER_A_ID:
        logger.error("FOLDER_A_ID is not set in .env file. Please set it to your Google Drive folder ID.")
        return
    
    try:
        service = authenticate()
        image_folder_id, hourly_folder_id, daily_folder_id, weekly_folder_id = get_folder_ids(service)
        hkt = pytz.timezone('Asia/Hong_Kong')
        now = datetime.now(hkt)
        logger.info(f"Current time (HKT): {now}")

        subfolder_ids = {
            'hourly': hourly_folder_id,
            'daily': daily_folder_id,
            'weekly': weekly_folder_id
        }

        video_types = [
            ('hourly', get_hourly_video_info),
            ('daily', get_daily_video_info),
            ('weekly', get_weekly_video_info)
        ]

        for video_type, get_info in video_types:
            start_time, end_time, video_name, desired_duration = get_info(now)
            subfolder_id = subfolder_ids[video_type]
            if video_exists(service, subfolder_id, video_name):
                logger.info(f"{video_type} video {video_name} already exists in {video_type} folder, skipping.")
                continue

            temp_dir = os.path.join(TEMP_DIR, video_type)
            os.makedirs(temp_dir, exist_ok=True)
            image_paths = download_images(service, image_folder_id, start_time, end_time, temp_dir)
            
            if len(image_paths) < IMAGE_THRESHOLDS[video_type]:
                logger.info(f"Not enough images for {video_type} video: {len(image_paths)} < {IMAGE_THRESHOLDS[video_type]}, skipping.")
                shutil.rmtree(temp_dir)
                continue

            output_video = os.path.join(TEMP_DIR, f"{video_type}_output.mp4")
            if create_video(image_paths, output_video, desired_duration):
                upload_video(service, subfolder_id, output_video, video_name)
                logger.info(f"Created and uploaded {video_type} video {video_name} to {video_type} folder")
                
                # Delete videos in lower-tier folders
                if video_type == 'daily':
                    delete_videos_in_folder(service, subfolder_ids['hourly'])
                elif video_type == 'weekly':
                    delete_videos_in_folder(service, subfolder_ids['daily'])
                    delete_old_images(service, image_folder_id, end_time)
            else:
                logger.error(f"Failed to create {video_type} video {video_name}")

            shutil.rmtree(temp_dir)
    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Cleaning up temporary directory...")
        shutil.rmtree(TEMP_DIR, ignore_errors=True)

if __name__ == '__main__':
    main()
