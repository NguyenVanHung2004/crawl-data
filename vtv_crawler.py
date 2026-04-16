import os
import requests
from bs4 import BeautifulSoup
import uuid
import re           
import datetime     
import random
import time
# Header để giả lập trình duyệt
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://vtv.vn/"
}
def get_category_id(slug):
    """
    Tự động lấy ZoneId từ slug (ví dụ: 'chinh-tri')
    """
    url = f"https://vtv.vn/{slug}.htm"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # Nhắm thẳng vào thẻ input hidden bạn vừa tìm thấy
        zone_input = soup.find("input", {"id": "hdZoneId"})
        if zone_input and zone_input.get("value"):
            return zone_input.get("value")
            
        return None
    except Exception as e:
        print(f"❌ Lỗi khi lấy ID chuyên mục {slug}: {e}")
        return None
def get_articles_from_timeline(category_id, start_page=1, num_pages=5):
    """
    Sử dụng endpoint timelinelist để lấy danh sách bài viết
    """
    all_articles = []
    
    for page in range(start_page, start_page + num_pages):
        # Link chuẩn bạn vừa tìm thấy
        api_url = f"https://vtv.vn/timelinelist/{category_id}/{page}.htm"
        
        print(f"📡 Đang quét API: {api_url}")
        
        try:
            resp = requests.get(api_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                print(f"⚠️ Trang {page} không phản hồi (Status: {resp.status_code})")
                break
                
            soup = BeautifulSoup(resp.content, "html.parser")
            
            # Tìm tất cả các thẻ article có class box-category-item
            items = soup.find_all("article", class_="box-category-item")
            
            if not items:
                print("🏁 Đã hết bài viết để lấy.")
                break
                
            for item in items:
                a_tag = item.find("a", class_="box-category-link-title")
                if not a_tag: continue
                
                href = a_tag.get('href')
                article_id = item.get('data-id') # Lấy ID trực tiếp từ thuộc tính data-id
                
                if href:
                    full_url = "https://vtv.vn" + href if not href.startswith("http") else href
                    if full_url not in [a['url'] for a in all_articles]:
                        all_articles.append({
                            "url": full_url,
                            "id": article_id
                        })
            
            print(f"✅ Trang {page}: Lấy được {len(items)} bài.")
            time.sleep(1) # Nghỉ 1s để MediaCDN không chặn
            
        except Exception as e:
            print(f"❌ Lỗi tại trang {page}: {e}")
            break
            
    return all_articles
def scrape_article(url, output_dir):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # 1. Lấy ID từ URL
        match_id = re.search(r'(\d+)\.htm$', url)
        if not match_id: return None
        article_id = match_id.group(1)
        
        # 2. Lấy Ngày tháng để dựng link TTS
        publish_date = soup.find("meta", property="article:published_time")
        if publish_date:
            date_str = publish_date['content'][:10].replace("-", "/") 
        else:
            date_str = datetime.datetime.now().strftime("%Y/%m/%d")

        # 2. Lấy Tiêu đề bài báo
        title_tag = soup.find("h1", class_="title-detail") or soup.find("meta", property="og:title")
        title = ""
        if title_tag:
            title = title_tag.get_text(strip=True) if title_tag.name == "h1" else title_tag.get("content", "")

        # 3. Nhắm thẳng vào "tọa độ" bạn vừa gửi
        # Thử selector itemprop trước vì nó là chuẩn SEO, rất ít khi đổi
        content_div = soup.find("div", {"itemprop": "articleBody"}) or \
                      soup.select_one(".detail-content") or \
                      soup.select_one(".noidung")

        if not content_div:
            print(f"⏩ Không tìm thấy articleBody cho: {article_id}")
            return None

        # 3. DỌN DẸP RÁC (Quan trọng để Dataset sạch)
        # Loại bỏ script, quảng cáo và chú thích ảnh (Caption thường không có trong file Audio đọc)
        for junk in content_div.select("script, style, .PhotoCMS_Caption, .link-lien-quan, div[id^='zone-']"):
            junk.decompose()

        # 4. Lấy các đoạn văn bản chính
        # Chỉ lấy text trong thẻ <p>, loại bỏ khoảng trắng thừa
        paragraphs = []
        for p in content_div.find_all("p"):
            txt = p.get_text(strip=True)
            if txt and len(txt) > 20: # Lọc bỏ các đoạn quá ngắn hoặc icon rác
                paragraphs.append(txt)
        
        standard_text = " ".join(paragraphs)

        if not standard_text:
            print(f"⏩ Văn bản sau khi lọc bị rỗng: {article_id}")
            return None

        VOICE_TAGS = [
            "vtv-nu-",      # Nữ miền Bắc (Mặc định)
            "vtv-nam-",     # Nam miền Bắc
            "vtv-nam-1-",   # Nam miền Nam
            "vtv-nu-1-"     # Nữ miền Nam
        ]
        selected_voice = random.choice(VOICE_TAGS)
        # 5. Xử lý Audio (Dựng link theo quy luật bạn tìm thấy)
        audio_src = f"https://tts.mediacdn.vn/{date_str}/{selected_voice}{article_id}.m4a"
        print(f"🎲 Chọn giọng: {selected_voice} cho bài {article_id}")
        
        # 6.Kiểm tra link audio
        check_resp = requests.head(audio_src, headers=HEADERS)
        if check_resp.status_code != 200:
            audio_src = audio_src.replace("vtv-nu", "vtv-nam")
            check_resp = requests.head(audio_src, headers=HEADERS)
            if check_resp.status_code != 200:
                print(f"⏩ Không có audio cho: {article_id}")
                return None

        # 8. Thiết lập đường dẫn lưu file
        os.makedirs(output_dir, exist_ok=True)
        ext = "m4a" # Theo link bạn cung cấp
        audio_path = os.path.join(output_dir, f"{article_id}.{ext}")
        text_path = os.path.join(output_dir, f"{article_id}.txt")
        meta_path = os.path.join(output_dir, f"{article_id}.json")

        # 9. Tải Audio và lưu Text + Metadata
        audio_data = requests.get(audio_src, headers=HEADERS).content
        with open(audio_path, "wb") as f:
            f.write(audio_data)
            
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(standard_text)

        import json
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({
                "id": article_id,
                "title": title,
                "url": url,
                "voice": selected_voice,
                "date": date_str
            }, f, ensure_ascii=False, indent=4)
            
        print(f"✅ Đã tải: {article_id} | Tiêu đề: {title[:30]}...")
        return {"id": article_id, "text": text_path, "audio": audio_path, "title": title, "url": url}
        
    except Exception as e:
        print(f"❌ Lỗi khi scrape {url}: {e}")
        return None
