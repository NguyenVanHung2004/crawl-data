import os
import json
import glob
import time
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from mutagen.mp4 import MP4
import datetime
import shutil

# --- CONFIGURATION ---
CLIENT_SECRETS_FILE = os.getenv('CLIENT_SECRETS_PATH', 'client_secrets.json')
TOKEN_FILE = os.getenv('TOKEN_PATH', 'token.json')
RAW_DATA_DIR = 'data/raw'
DATASET_DIR = 'data/dataset'
# Lấy ID từ Env Var nếu có, nếu không lấy giá trị mặc định
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '1SX288j2UVbIOvNxLbV-ak2_HK8QOSUs930LMBnJ5-WI') 
SHEET_TITLE = "VTV Crawl Dataset Statistics"

# SCOPES for Google Sheets and Drive
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

def get_audio_duration(file_path):
    """Returns duration in seconds using mutagen."""
    try:
        audio = MP4(file_path)
        return round(audio.info.length, 2)
    except Exception as e:
        # print(f"⚠️ Could not read duration for {file_path}: {e}")
        return 0

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

def authenticate():
    # --- DEPLOYMENT SUPPORT: Initialize files from Env Vars ---
    env_secrets = os.getenv('GOOGLE_CLIENT_SECRETS')
    if env_secrets and not os.path.exists(CLIENT_SECRETS_FILE):
        print("🛠️ Initializing client_secrets.json from Environment Variable...")
        with open(CLIENT_SECRETS_FILE, 'w') as f:
            f.write(env_secrets)
            
    env_token = os.getenv('GOOGLE_TOKEN')
    if env_token and not os.path.exists(TOKEN_FILE):
        print("🛠️ Initializing token.json from Environment Variable...")
        with open(TOKEN_FILE, 'w') as f:
            f.write(env_token)
    # ----------------------------------------------------------

    creds = None
    # The file token.json stores the user's access and refresh tokens
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CLIENT_SECRETS_FILE):
                raise FileNotFoundError(f"Missing {CLIENT_SECRETS_FILE}. Please follow instructions to create it.")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            
        # Save the credentials for the next run
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
            
    return creds

# --- DRIVE HELPERS ---
def get_or_create_folder(service, folder_name, parent_id=None):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if files:
        return files[0]['id']
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        file = service.files().create(body=file_metadata, fields='id').execute()
        return file.get('id')

def upload_file_to_drive(service, file_path, folder_id, overwrite=False):
    file_name = os.path.basename(file_path)
    # Check if file exists
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    
    for attempt in range(3):
        try:
            results = service.files().list(q=query, fields="files(id, webViewLink)").execute()
            files = results.get('files', [])
            
            if files:
                file_id = files[0]['id']
                if overwrite:
                    # Update existing file content
                    media = MediaFileUpload(file_path, resumable=True)
                    service.files().update(fileId=file_id, media_body=media).execute()
                    return files[0]['webViewLink']
                else:
                    return files[0]['webViewLink']
            
            file_metadata = {'name': file_name, 'parents': [folder_id]}
            media = MediaFileUpload(file_path, resumable=True)
            file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
            
            # Set public permission
            service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
            
            return file.get('webViewLink')
        except Exception as e:
            if attempt < 2:
                print(f"  ⚠️ Network issue ({e}), retrying in 5s... (Attempt {attempt+1}/3)")
                time.sleep(5)
            else:
                raise e

# --- SHEETS HELPERS ---
def create_spreadsheet(service, title):
    spreadsheet = {'properties': {'title': title}}
    spreadsheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
    sheet_id = spreadsheet.get('spreadsheetId')
    print(f"✨ Created new spreadsheet: https://docs.google.com/spreadsheets/d/{sheet_id}")
    return sheet_id

def get_first_sheet_name(service, spreadsheet_id):
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return spreadsheet['sheets'][0]['properties']['title']

def setup_headers(service, spreadsheet_id, sheet_name):
    headers = [["Article ID", "Category", "Title", "Source URL", "Duration (s)", "Word Count", "Crawl Date", "Audio Link (Drive)", "Text Link (Drive)", "Dataset Folder (Drive)"]]
    body = {'values': headers}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'!A1:J1",
        valueInputOption="RAW", body=body).execute()

def get_existing_data(service, spreadsheet_id, sheet_name):
    """Returns a dict of {id: row_index} for existing entries."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'!A:J").execute()
        values = result.get('values', [])
        data_map = {}
        for i, row in enumerate(values):
            if row:
                data_map[row[0]] = {
                    'row_idx': i + 1,
                    'has_links': len(row) >= 10 and row[9] != ""
                }
        return data_map
    except Exception:
        return {}

def sync():
    print("🔍 Initializing Google Services...")
    creds = authenticate()
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # In thông tin tài khoản đang sử dụng
    print(f"✅ Authenticated successfully.")
    
    global SPREADSHEET_ID
    if not SPREADSHEET_ID:
        try:
            SPREADSHEET_ID = create_spreadsheet(sheets_service, SHEET_TITLE)
        except Exception as e:
            if "403" in str(e):
                print("\n❌ AUTH Error (403): Cannot create file. Follow manual instructions.")
                return
            raise e
    
    sheet_name = get_first_sheet_name(sheets_service, SPREADSHEET_ID)
    existing_data = get_existing_data(sheets_service, SPREADSHEET_ID, sheet_name)
    if not existing_data:
        setup_headers(sheets_service, SPREADSHEET_ID, sheet_name)
        existing_data = get_existing_data(sheets_service, SPREADSHEET_ID, sheet_name)

    # Drive Folder Setup
    print("📁 Preparing Drive folder structure...")
    parent_folder_id = get_or_create_folder(drive_service, "VTV_Dataset")
    
    # Lấy link folder cha để người dùng vào kiểm tra
    parent_meta = drive_service.files().get(fileId=parent_folder_id, fields='webViewLink').execute()
    print(f"✨ Drive Folder: {parent_meta.get('webViewLink')}")
    print(f"👉 HÃY CHIA SẺ (SHARE) folder trên với email của bạn để xem file.")

    raw_root_id = get_or_create_folder(drive_service, "raw", parent_folder_id)
    dataset_root_id = get_or_create_folder(drive_service, "dataset", parent_folder_id)

    meta_files = glob.glob(os.path.join(RAW_DATA_DIR, "**/*.json"), recursive=True)
    rows_to_append = []
    synced_paths = [] # Danh sách các file/folder cần xóa sau khi sync xong
    
    for meta_path in meta_files:
        with open(meta_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        article_id = data.get('id')
        is_update = False
        if article_id in existing_data:
            if existing_data[article_id]['has_links']:
                continue
            else:
                is_update = True # Cần upload bù link Drive
            
        category_name = os.path.basename(os.path.dirname(meta_path))
        print(f"📤 Uploading Article {article_id} to Drive...")
        
        # Drive subfolder for category
        cat_folder_id = get_or_create_folder(drive_service, category_name, raw_root_id)
        
        # Audio & Text files
        audio_path = meta_path.replace('.json', '.m4a')
        text_path = meta_path.replace('.json', '.txt')
        
        audio_link = ""
        if os.path.exists(audio_path):
            audio_link = upload_file_to_drive(drive_service, audio_path, cat_folder_id)
            
        text_link = ""
        if os.path.exists(text_path):
            text_link = upload_file_to_drive(drive_service, text_path, cat_folder_id)
        
        # Dataset folder upload
        dataset_folder_link = ""
        local_dataset_path = os.path.join(DATASET_DIR, category_name, f"{article_id}_audio")
        if os.path.exists(local_dataset_path):
            print(f"📁 Uploading Dataset folder for {article_id}...")
            cat_dataset_root_id = get_or_create_folder(drive_service, category_name, dataset_root_id)
            article_dataset_folder_id = get_or_create_folder(drive_service, f"{article_id}_audio", cat_dataset_root_id)
            # Set public for the folder
            drive_service.permissions().create(fileId=article_dataset_folder_id, body={'type': 'anyone', 'role': 'reader'}).execute()
            
            # Fetch folder link
            folder_meta = drive_service.files().get(fileId=article_dataset_folder_id, fields='webViewLink').execute()
            dataset_folder_link = folder_meta.get('webViewLink')
            
            # Upload all files in dataset folder
            chunk_drive_links = {}
            for f in os.listdir(local_dataset_path):
                f_path = os.path.join(local_dataset_path, f)
                if os.path.isfile(f_path):
                    link = upload_file_to_drive(drive_service, f_path, article_dataset_folder_id)
                    if link:
                        chunk_drive_links[f] = link

            # Update jsonl file with individual chunk drive URLs
            jsonl_path = os.path.join(DATASET_DIR, category_name, "metadata.jsonl")
            if os.path.exists(jsonl_path) and chunk_drive_links:
                try:
                    updated_lines = []
                    with open(jsonl_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            try:
                                item = json.loads(line)
                                audio_file_name = os.path.basename(item.get("audio_file", ""))
                                if audio_file_name in chunk_drive_links:
                                    item["chunk_drive_url"] = chunk_drive_links[audio_file_name]
                                updated_lines.append(json.dumps(item, ensure_ascii=False))
                            except:
                                updated_lines.append(line.strip())
                    
                    with open(jsonl_path, 'w', encoding='utf-8') as f:
                        for line in updated_lines:
                            f.write(line + "\n")
                except Exception as e:
                    print(f"  ⚠️ Error updating metadata.jsonl with chunk links: {e}")

        duration = get_audio_duration(audio_path) if os.path.exists(audio_path) else 0
        word_count = 0
        if os.path.exists(text_path):
            with open(text_path, 'r', encoding='utf-8') as tf:
                word_count = len(tf.read().split())
        
        if is_update:
            # Update specific row in Sheets
            row_idx = existing_data[article_id]['row_idx']
            print(f"🔄 Updating links for row {row_idx}...")
            update_body = {'values': [[audio_link, text_link, dataset_folder_link]]}
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!H{row_idx}:J{row_idx}",
                valueInputOption="RAW", body=update_body).execute()
            # Xóa ngay vì đã update thành công từng dòng
            clean_local_article(meta_path, audio_path, text_path, local_dataset_path)
        else:
            rows_to_append.append([
                article_id, category_name, data.get('title', 'N/A'), 
                data.get('url', 'N/A'), duration, word_count, data.get('date', 'N/A'),
                audio_link, text_link, dataset_folder_link
            ])
            synced_paths.append((meta_path, audio_path, text_path, local_dataset_path))

    # --- NEW: Upload Aggregate metadata.jsonl for each category ---
    print("📜 Syncing aggregate metadata.jsonl files...")
    all_jsonls = glob.glob(os.path.join(DATASET_DIR, "**/metadata.jsonl"), recursive=True)
    for jsonl_path in all_jsonls:
        cat_name = os.path.basename(os.path.dirname(jsonl_path))
        print(f"  📤 Updating metadata.jsonl for {cat_name}...")
        cat_dataset_root_id = get_or_create_folder(drive_service, cat_name, dataset_root_id)
        upload_file_to_drive(drive_service, jsonl_path, cat_dataset_root_id, overwrite=True)

    if rows_to_append:
        print(f"🚀 Updating Google Sheets with {len(rows_to_append)} new entries...")
        body = {'values': rows_to_append}
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A1",
            valueInputOption="RAW", body=body).execute()
        
        # Xóa các file đã sync thành công
        for paths in synced_paths:
            clean_local_article(*paths)
            
        print("✅ Sync and Cleanup complete!")
    else:
        print("🙌 No new articles to sync (but metadata.jsonl updated).")

def clean_local_article(meta_path, audio_path, text_path, dataset_path):
    """Xóa các file local sau khi đã sync xong."""
    try:
        if os.path.exists(meta_path): os.remove(meta_path)
        if os.path.exists(audio_path): os.remove(audio_path)
        if os.path.exists(text_path): os.remove(text_path)
        if os.path.exists(dataset_path): shutil.rmtree(dataset_path)
        # print(f"  🗑️ Deleted local files for {os.path.basename(meta_path)}")
    except Exception as e:
        print(f"  ⚠️ Error deleting local files: {e}")

def get_remote_ids():
    if not SPREADSHEET_ID: return set()
    try:
        creds = authenticate()
        service = build('sheets', 'v4', credentials=creds)
        sheet_name = get_first_sheet_name(service, SPREADSHEET_ID)
        data = get_existing_data(service, SPREADSHEET_ID, sheet_name)
        return set(data.keys())
    except Exception:
        return set()

if __name__ == "__main__":
    try:
        sync()
    except Exception as e:
        print(f"❌ Error: {e}")
