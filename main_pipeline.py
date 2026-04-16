import os
import time
import random
from vtv_crawler import get_articles_from_timeline, scrape_article, get_category_id
from audio_aligner import load_recognizer, process_and_align
from google_sheets_sync import sync as sync_to_google, get_remote_ids
from pydub import AudioSegment

# Danh sách tất cả các slug "sạch" bạn đã tìm thấy
VTV_CATEGORIES = [
    "chinh-tri", "xa-hoi", "phap-luat", "the-gioi", 
    "kinh-te", "y-te", "doi-song", "cong-nghe", "giao-duc"
]

START_PAGE = 1 # Hôm nay đào từ trang 50 trở đi
NUM_PAGES = 10  # Đào 10 trang (tầm 200 bài)


def main():
    print("🚀 STARTING MASTER VTV DATASET PIPELINE...")
    
    RAW_BASE_DIR = "data/raw"
    DATASET_BASE_DIR = "data/dataset"
    NUM_PAGES_PER_CAT = 1  # Mỗi chuyên mục lấy 3 trang (~60 bài)
    
    # 1. Load Model ZipFormer (Load 1 lần duy nhất)
    print("1. Loading ZipFormer model...")
    recognizer = load_recognizer()
    if not recognizer: return

    # 1.5. Lấy danh sách ID đã cào trên Google Sheets để tránh cào lại
    print("🔍 Fetching already crawled IDs from Google Sheets...")
    blacklisted_ids = get_remote_ids()
    print(f"✅ Found {len(blacklisted_ids)} IDs on Google. These will be skipped.")

    # 2. Vòng lặp quét qua từng Chuyên mục
    for slug in VTV_CATEGORIES:
        print(f"\n" + "="*50)
        print(f"📂 PROCESSING CATEGORY: {slug.upper()}")
        print("="*50)
        
        # Lấy ZoneId tự động
        cat_id = get_category_id(slug)
        if not cat_id:
            print(f"⏩ Skip {slug}: Could not find ZoneId")
            continue
            
        # Tạo thư mục riêng cho từng chuyên mục
        cat_raw_dir = os.path.join(RAW_BASE_DIR, slug)
        cat_dataset_dir = os.path.join(DATASET_BASE_DIR, slug)
        os.makedirs(cat_raw_dir, exist_ok=True)
        os.makedirs(cat_dataset_dir, exist_ok=True)

        # Lấy danh sách bài viết từ Timeline
        articles = get_articles_from_timeline(cat_id, start_page=START_PAGE, num_pages=NUM_PAGES)
        print(f"✅ Found {len(articles)} potential articles in {slug}")

        # 3. Vòng lặp xử lý từng bài báo trong chuyên mục
        for item in articles:
            article_id = item['id']
            url = item['url']
            
            # Kiểm tra nếu đã xử lý rồi hoặc đã có trên Google Sheets thì bỏ qua
            check_path = os.path.join(cat_dataset_dir, f"{article_id}_audio")
            if os.path.exists(check_path) or article_id in blacklisted_ids:
                print(f"⏩ Article {article_id} already exists (Local or Google). Skipping...")
                continue

            # A. Crawl Audio & Text
            scrape_result = scrape_article(url, cat_raw_dir)
            if not scrape_result: continue
            
            # B. Convert sang WAV 16kHz
            audio_path = scrape_result["audio"]
            text_path = scrape_result["text"]
            wav_path = audio_path.replace(".m4a", ".wav")
            
            try:
                audio = AudioSegment.from_file(audio_path)
                audio = audio.set_frame_rate(16000).set_channels(1)
                audio.export(wav_path, format="wav")
            except Exception as e:
                print(f"❌ Audio Error: {e}")
                continue
            
            # C. Alignment (Cắt thành câu nhỏ)
            print(f"🎬 Aligning {article_id}...")
            try:
                process_and_align(wav_path, text_path, check_path, article_id, recognizer)
                # Xóa file WAV trung gian cho nhẹ máy
                if os.path.exists(wav_path): os.remove(wav_path)
            except Exception as e:
                print(f"❌ Alignment Error: {e}")

            # Nghỉ ngắn giữa các bài để "giả làm người dùng"
            time.sleep(random.uniform(1.0, 2.5))

        print(f"✔️ Finished Category: {slug}")
        # Nghỉ dài sau mỗi chuyên mục để IP được "thở"
        time.sleep(5)

    print("\n🏆 MISSION ACCOMPLISHED: All categories processed!")
    
    # 4. Sync to Google Sheets
    print("\n📊 SYNCING DATA TO GOOGLE SHEETS...")
    try:
        sync_to_google()
    except Exception as e:
        print(f"❌ Google Sync Error: {e}")
    else:
        print("🙌 No new articles to sync.")

if __name__ == "__main__":
    main()