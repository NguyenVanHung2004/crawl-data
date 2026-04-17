import os
import time
import random
import gc
import queue
import threading
from vtv_crawler import get_articles_from_timeline, scrape_article, get_category_id
from audio_aligner import load_recognizer, process_and_align
from google_sheets_sync import sync as sync_to_google, get_remote_ids

# --- CONFIG ---
VTV_CATEGORIES = [
    "chinh-tri", "xa-hoi", "phap-luat", "the-gioi", 
    "kinh-te", "y-te", "doi-song", "cong-nghe", "giao-duc"
]
START_PAGE = 6
NUM_PAGES = 10  
RAW_BASE_DIR = "data/raw"
DATASET_BASE_DIR = "data/dataset"
SYNC_INTERVAL = 5 

# Queue kết nối Crawler và Aligner
article_queue = queue.Queue(maxsize=15) 

def crawler_worker(blacklisted_ids):
    """Luồng chuyên đi cào bài và tải audio (Producer)"""
    print("[CRAWLER] Bắt đầu quét dữ liệu...")
    for slug in VTV_CATEGORIES:
        print(f"\n[CRAWLER] PROCESSING CATEGORY: {slug.upper()}")
        cat_id = get_category_id(slug)
        if not cat_id: continue
            
        cat_raw_dir = os.path.join(RAW_BASE_DIR, slug)
        cat_dataset_dir = os.path.join(DATASET_BASE_DIR, slug)
        os.makedirs(cat_raw_dir, exist_ok=True)
        os.makedirs(cat_dataset_dir, exist_ok=True)

        articles = get_articles_from_timeline(cat_id, start_page=START_PAGE, num_pages=NUM_PAGES)
        
        for item in articles:
            article_id = item['id']
            url = item['url']
            
            check_path = os.path.join(cat_dataset_dir, f"{article_id}_audio")
            if os.path.exists(check_path) or article_id in blacklisted_ids:
                continue

            # Tải bài báo và m4a luôn trong luồng này
            scrape_result = scrape_article(url, cat_raw_dir)
            if scrape_result:
                # Đẩy data vào hàng đợi để luồng AI xử lý
                article_queue.put({
                    "data": scrape_result,
                    "dataset_dir": cat_dataset_dir,
                    "check_path": check_path
                })
            
            # GIẢM SLEEP XUỐNG 0.5s để tăng tốc cào bài
            time.sleep(random.uniform(0.3, 0.7))

    # Gửi tín hiệu kết thúc cho Consumer
    article_queue.put(None)
    print("[CRAWLER] Hoàn thành việc quét toàn bộ chuyên mục.")

def aligner_worker(recognizer):
    """Luồng chuyên chạy AI và Sync Google (Consumer)"""
    print("[ALIGNER] Sẵn sàng xử lý AI...")
    processed_count = 0
    
    while True:
        task = article_queue.get()
        if task is None: # Tín hiệu kết thúc
            break
            
        article_id = task["data"]["id"]
        audio_path = task["data"]["audio"]
        text_path = task["data"]["text"]
        check_path = task["check_path"]

        print(f"[ALIGNER] Đang chạy AI cho bài: {article_id}")
        try:
            # Chạy ZipFormer & Alignment
            process_and_align(audio_path, text_path, check_path, article_id, recognizer)
            processed_count += 1
            
            # Sync định kỳ mỗi 5 bài
            if processed_count % SYNC_INTERVAL == 0:
                print(f"[SYNC] Tu dong dong bo {processed_count} bai len Google Sheets...")
                try:
                    sync_to_google()
                except Exception as e:
                    print(f"Sync Error: {e}")
        except Exception as e:
            print(f"❌ Lỗi xử lý bài {article_id}: {e}")
        finally:
            gc.collect()
            article_queue.task_done()

    # Đồng bộ lần cuối khi kết thúc
    print("\n[SYNC] Đang đồng bộ lần cuối cùng...")
    try:
        sync_to_google()
    except Exception as e:
        print(f"Final Sync Error: {e}")
    print("[ALIGNER] Hoàn thành mọi nhiệm vụ.")

def main():
    print("--- STARTING MULTI-THREADED VTV PIPELINE (SUPER FAST MODE) ---")
    
    # 1. Load Model (Load 1 lần duy nhất ở main thread)
    recognizer = load_recognizer()
    if not recognizer: return

    # 2. Lấy danh sách ID đã có để tránh trùng
    blacklisted_ids = get_remote_ids()
    print(f"Bỏ qua {len(blacklisted_ids)} bài báo đã có trên Google Sheets.")

    # 3. Khởi chạy 2 luồng song song
    t1 = threading.Thread(target=crawler_worker, args=(blacklisted_ids,), name="CrawlerThread")
    t2 = threading.Thread(target=aligner_worker, args=(recognizer,), name="AlignerThread")

    t1.start()
    t2.start()

    # Đợi cả 2 luồng hoàn thành
    t1.join()
    t2.join()

    print("\n[FINISH] Toàn bộ quy trình đã hoàn tất.")

if __name__ == "__main__":
    main()