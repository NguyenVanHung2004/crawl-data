import os
import time
import random
import gc
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
NUM_PAGES = 5  # Đào 10 trang (tầm 200 bài)


def main():
    print("--- STARTING MASTER VTV DATASET PIPELINE ---")
    
    RAW_BASE_DIR = "data/raw"
    DATASET_BASE_DIR = "data/dataset"
    NUM_PAGES_PER_CAT = 1  
    SYNC_INTERVAL = 5 # Sync every 5 articles
    processed_count = 0
    
    # 1. Load Model ZipFormer
    print("1. Loading ZipFormer model...")
    recognizer = load_recognizer()
    if not recognizer: return

    # 1.5. Check existing IDs
    print("Checking already crawled IDs from Google Sheets...")
    blacklisted_ids = get_remote_ids()
    print(f"Done. Found {len(blacklisted_ids)} IDs on Google. These will be skipped.")

    # 2. Category Loop
    for slug in VTV_CATEGORIES:
        print(f"\n" + "="*50)
        print(f"PROCESSING CATEGORY: {slug.upper()}")
        print("="*50)
        
        cat_id = get_category_id(slug)
        if not cat_id:
            print(f"Skip {slug}: Could not find ZoneId")
            continue
            
        cat_raw_dir = os.path.join(RAW_BASE_DIR, slug)
        cat_dataset_dir = os.path.join(DATASET_BASE_DIR, slug)
        os.makedirs(cat_raw_dir, exist_ok=True)
        os.makedirs(cat_dataset_dir, exist_ok=True)

        articles = get_articles_from_timeline(cat_id, start_page=START_PAGE, num_pages=NUM_PAGES)
        print(f"Found {len(articles)} potential articles in {slug}")

        # 3. Article Loop
        for item in articles:
            article_id = item['id']
            url = item['url']
            
            check_path = os.path.join(cat_dataset_dir, f"{article_id}_audio")
            if os.path.exists(check_path) or article_id in blacklisted_ids:
                print(f"Article {article_id} already exists (Local or Google). Skipping...")
                continue

            # A. Scrape
            scrape_result = scrape_article(url, cat_raw_dir)
            if not scrape_result: continue
            
            # B. Get Paths
            audio_path = scrape_result["audio"]
            text_path = scrape_result["text"]
            
            # C. Alignment
            print(f"Aligning {article_id}...")
            try:
                process_and_align(audio_path, text_path, check_path, article_id, recognizer)
                processed_count += 1
                
                # --- SYNC AFTER EVERY N ARTICLES ---
                if processed_count % SYNC_INTERVAL == 0:
                    print(f"\n📊 Auto-Syncing after {processed_count} articles...")
                    try:
                        sync_to_google()
                    except Exception as e:
                        print(f"Sync Error: {e}")
                
            except Exception as e:
                print(f"Alignment Error: {e}")
            finally:
                gc.collect()

            time.sleep(random.uniform(1.0, 2.5))

        print(f"Finished Category: {slug}")
        time.sleep(5)

    # Final Sync
    print("\nMISSION ACCOMPLISHED: All categories processed!")
    print("Final Sync to Google Sheets...")
    try:
        sync_to_google()
    except Exception as e:
        print(f"Final Sync Error: {e}")
    else:
        print("Done. All data synced.")

if __name__ == "__main__":
    main()