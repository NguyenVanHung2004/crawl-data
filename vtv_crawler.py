import os
import requests
from bs4 import BeautifulSoup
import uuid
import re           
import datetime     
import random
import time
import json
from mutagen.mp4 import MP4
from mutagen.mp3 import MP3

# Header để giả lập trình duyệt
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://vtv.vn/"
}

def get_audio_duration(file_path):
    """Lấy độ dài file audio (giây) dùng mutagen"""
    try:
        if file_path.endswith('.m4a'):
            audio = MP4(file_path)
        elif file_path.endswith('.mp3'):
            audio = MP3(file_path)
        else:
            return 0
        return audio.info.length
    except Exception as e:
        print(f"⚠️ Không thể lấy độ dài audio {file_path}: {e}")
        return 0

def safe_requests(method, url, **kwargs):
    """
    Hàm wrapper cho requests để tự động thử lại khi gặp lỗi Timeout hoặc Net
    """
    retries = 3
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 30
        
    for i in range(retries):
        try:
            if method.lower() == 'get':
                resp = requests.get(url, **kwargs)
            elif method.lower() == 'head':
                resp = requests.head(url, **kwargs)
            else:
                resp = requests.request(method, url, **kwargs)
                
            if resp.status_code != 429:
                return resp
            
            wait_time = (i + 1) * 5
            print(f"Blocked (429) at {url}. Wait {wait_time}s...")
            time.sleep(wait_time)
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if i < retries - 1:
                wait_time = (i + 1) * 3
                print(f"Timeout/Error at {url}. Retry {i+2} after {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    return None

def get_category_id(slug):
    url = f"https://vtv.vn/{slug}.htm"
    try:
        resp = safe_requests("get", url, headers=HEADERS, timeout=30)
        if not resp: return None
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        
        zone_input = soup.find("input", {"id": "hdZoneId"})
        if zone_input and zone_input.get("value"):
            return zone_input.get("value")
            
        return None
    except Exception as e:
        print(f"❌ Lỗi khi lấy ID chuyên mục {slug}: {e}")
        return None

def get_articles_from_timeline(category_id, start_page=1, num_pages=5):
    all_articles = []
    
    for page in range(start_page, start_page + num_pages):
        api_url = f"https://vtv.vn/timelinelist/{category_id}/{page}.htm"
        print(f"Scanning articles {api_url}")
        
        try:
            resp = safe_requests("get", api_url, headers=HEADERS, timeout=30)
            if not resp:
                print(f"⚠️ Không thể tải dữ liệu từ trang {page}")
                break
            
            if resp.status_code != 200:
                print(f"⚠️ Trang {page} không phản hồi (Status: {resp.status_code})")
                break
                
            soup = BeautifulSoup(resp.content, "html.parser")
            items = soup.find_all("article", class_="box-category-item")
            
            if not items:
                print("🏁 Đã hết bài viết để lấy.")
                break
                
            for item in items:
                a_tag = item.find("a", class_="box-category-link-title")
                if not a_tag: continue
                
                href = a_tag.get('href')
                article_id = item.get('data-id') 
                
                if href:
                    full_url = "https://vtv.vn" + href if not href.startswith("http") else href
                    if full_url not in [a['url'] for a in all_articles]:
                        all_articles.append({
                            "url": full_url,
                            "id": article_id
                        })
            
            print(f"Page {page}: Got {len(items)} articles.")
            time.sleep(0.5) 
            
        except Exception as e:
            print(f"❌ Lỗi tại trang {page}: {e}")
            break
            
    return all_articles

def scrape_article(url, output_dir):
    try:
        resp = safe_requests("get", url, headers=HEADERS, timeout=60)
        if not resp: return None
        soup = BeautifulSoup(resp.content, "html.parser")
        
        match_id = re.search(r'(\d+)\.htm$', url)
        if not match_id: return None
        article_id = match_id.group(1)
        
        publish_date = soup.find("meta", property="article:published_time")
        if publish_date:
            date_str = publish_date['content'][:10].replace("-", "/") 
        else:
            date_str = datetime.datetime.now().strftime("%Y/%m/%d")

        title_tag = soup.find("h1", class_="title-detail") or soup.find("meta", property="og:title")
        title = title_tag.get_text(strip=True) if title_tag and title_tag.name == "h1" else (title_tag.get("content", "") if title_tag else "")

        content_div = soup.find("div", {"itemprop": "articleBody"}) or \
                      soup.select_one(".detail-content") or \
                      soup.select_one(".noidung")

        if not content_div:
            return None

        for junk in content_div.select("script, style, .PhotoCMS_Caption, .link-lien-quan, div[id^='zone-']"):
            junk.decompose()

        paragraphs = []
        for p in content_div.find_all("p"):
            txt = p.get_text(strip=True)
            if txt and len(txt) > 20: 
                paragraphs.append(txt)
        
        standard_text = " ".join(paragraphs)

        if not standard_text:
            return None

        VOICE_TAGS = ["vtv-nu-", "vtv-nam-", "vtv-nam-1-", "vtv-nu-1-"]
        selected_voice = random.choice(VOICE_TAGS)
        audio_src = f"https://tts.mediacdn.vn/{date_str}/{selected_voice}{article_id}.m4a"
        
        check_resp = safe_requests("head", audio_src, headers=HEADERS, timeout=15)
        if not check_resp or check_resp.status_code != 200:
            audio_src = audio_src.replace("vtv-nu", "vtv-nam")
            check_resp = safe_requests("head", audio_src, headers=HEADERS, timeout=15)
            if not check_resp or check_resp.status_code != 200:
                return None

        os.makedirs(output_dir, exist_ok=True)
        audio_path = os.path.join(output_dir, f"{article_id}.m4a")
        text_path = os.path.join(output_dir, f"{article_id}.txt")
        meta_path = os.path.join(output_dir, f"{article_id}.json")

        audio_resp = safe_requests("get", audio_src, headers=HEADERS, timeout=60)
        if not audio_resp:
            return None
        
        with open(audio_path, "wb") as f:
            f.write(audio_resp.content)
            
        # KIỂM TRA ĐỘ DÀI: CHỈ LẤY TRÊN 5 PHÚT (300 GIÂY)
        duration = get_audio_duration(audio_path)
        if duration < 300:
            print(f"Skip {article_id}: Audio too short ({duration:.1f}s < 300s)")
            if os.path.exists(audio_path): os.remove(audio_path)
            return None
        else:
            print(f"🎵 Nhận bài {article_id}: Độ dài {duration/60:.1f} phút")

        with open(text_path, "w", encoding="utf-8") as f:
            f.write(standard_text)

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({
                "id": article_id, "title": title, "url": url,
                "voice": selected_voice, "date": date_str, "duration": duration
            }, f, ensure_ascii=False, indent=4)
            
        print(f"✅ Đã tải: {article_id} | Tiêu đề: {title[:30]}...")
        return {"id": article_id, "text": text_path, "audio": audio_path, "title": title, "url": url}
        
    except Exception as e:
        print(f"❌ Lỗi khi scrape {url}: {e}")
        return None
