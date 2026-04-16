import os
import wave
import json
import requests
import numpy as np
import sherpa_onnx
import re
from pydub import AudioSegment
from underthesea import sent_tokenize
# --- Cấu hình ---
# Cho phép đổi đường dẫn qua biến môi trường, mặc định là thư mục models trong dự án
MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.getcwd(), "models"))

def download_model_if_needed():
    """Tự động tải model ZipFormer từ HuggingFace nếu chưa có (phục vụ deploy cloud)."""
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    base_url = "https://huggingface.co/k2-fsa/sherpa-onnx-offline-zipformer-vi-2023-09-04/resolve/main"
    files = {
        "tokens.txt": f"{base_url}/tokens.txt",
        "encoder-epoch-12-avg-8.int8.onnx": f"{base_url}/encoder-epoch-12-avg-8.int8.onnx",
        "decoder-epoch-12-avg-8.onnx": f"{base_url}/decoder-epoch-12-avg-8.onnx",
        "joiner-epoch-12-avg-8.int8.onnx": f"{base_url}/joiner-epoch-12-avg-8.int8.onnx"
    }

    print(f"📂 Checking model in: {MODEL_DIR}")
    for filename, url in files.items():
        file_path = os.path.join(MODEL_DIR, filename)
        if not os.path.exists(file_path):
            print(f"📥 Downloading {filename} from HuggingFace...")
            try:
                resp = requests.get(url, stream=True, timeout=30)
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✅ Downloaded {filename}")
            except Exception as e:
                print(f"❌ Error downloading {filename}: {e}")
                if os.path.exists(file_path): os.remove(file_path)

def load_recognizer():
    # Kiểm tra và tải model nếu cần
    download_model_if_needed()
    
    # Giữ nguyên hàm load của bạn
    recognizer = sherpa_onnx.OfflineRecognizer.from_transducer(
        tokens=os.path.join(MODEL_DIR, "tokens.txt"),
        encoder=os.path.join(MODEL_DIR, "encoder-epoch-12-avg-8.int8.onnx"),
        decoder=os.path.join(MODEL_DIR, "decoder-epoch-12-avg-8.onnx"),
        joiner=os.path.join(MODEL_DIR, "joiner-epoch-12-avg-8.int8.onnx"),
        num_threads=4,
        sample_rate=16000,
        feature_dim=80,
    )
    return recognizer

def clean_token(t):
    # ZipFormer thường có ký tự lạ hoặc khoảng trắng ở đầu token (ví dụ: ' ', '▁')
    return t.replace(' ', '').replace('▁', '').strip().lower()

def process_and_align(audio_path, text_path, output_dir, article_id, recognizer):
    # 1. Chuẩn hóa Audio
    audio = AudioSegment.from_file(audio_path).set_frame_rate(16000).set_channels(1)
    wav_16k = audio_path.replace(".m4a", "_16k.wav")
    audio.export(wav_16k, format="wav")

    # 2. Nhận dạng lấy Timestamps (ASR)
    # Để tránh Bad Allocation cho file dài, ta sẽ decode từng đoạn 30s 
    # Nhưng ở đây mình sẽ dùng logic gộp kết quả để lấy full timestamps
    with wave.open(wav_16k, 'rb') as f:
        samples = np.frombuffer(f.readframes(f.getnframes()), dtype=np.int16).astype(np.float32) / 32768.0

    # Nếu file quá dài, chia samples thành các đoạn nhỏ để decode
    # (Ví dụ chia mỗi 30s ~ 480,000 samples)
    chunk_size = 16000 * 30 
    all_tokens = []
    all_timestamps = []
    offset = 0

    for i in range(0, len(samples), chunk_size):
        chunk = samples[i : i + chunk_size]
        stream = recognizer.create_stream()
        stream.accept_waveform(16000, chunk)
        recognizer.decode_stream(stream)
        
        result = stream.result
        # Cộng offset thời gian cho từng đoạn chunk
        for t_idx, ts in enumerate(result.timestamps):
            all_tokens.append(clean_token(result.tokens[t_idx]))
            all_timestamps.append(ts + (offset / 16000))
        offset += len(chunk)

    # 3. Đọc văn bản gốc và tách câu
    with open(text_path, 'r', encoding='utf-8') as f:
        standard_text = f.read()
    sentences = sent_tokenize(standard_text)
    # =============================================================
    # GIAI ĐOẠN 3.5: TIỀN XỬ LÝ TOKENS (Gộp đánh vần)
    # =============================================================
    processed_tokens = []
    processed_timestamps = []
    temp_word = ""
    
    print(f"🧹 Đang tiền xử lý {len(all_tokens)} tokens từ ZipFormer...")
    
    for i in range(len(all_tokens)):
        t = all_tokens[i]
        # Nếu là ký tự đơn lẻ (ZipFormer đánh vần) thì gộp lại
        # Ví dụ: 'm', 'ư', 'ờ', 'i' -> 'mười'
        if len(t) == 1 and i < len(all_tokens) - 1:
            temp_word += t
        else:
            final_t = temp_word + t
            processed_tokens.append(final_t)
            # Lấy timestamp của ký tự đầu tiên trong cụm đánh vần
            processed_timestamps.append(all_timestamps[i - len(temp_word)])
            temp_word = ""
    
    print(f"✅ Đã dọn dẹp xong: Còn {len(processed_tokens)} từ hoàn chỉnh.")
    # =============================================================
    # 4. KHỚP TIMESTAMP VÀO CÂU (Hybrid Monotonic Alignment)
    # =============================================================
    from rapidfuzz import fuzz
    import re

    os.makedirs(output_dir, exist_ok=True)
    metadata = []
    
    # Biến quan trọng: Con trỏ token luôn tiến về phía trước, không bao giờ quay đầu
    last_token_idx = 0 
    
    print(f"\n--- 🎯 BẮT ĐẦU ALIGNMENT CHI TIẾT ({len(sentences)} câu) ---")

    for idx, sent in enumerate(sentences):
        # 4.1 Tiền xử lý văn bản câu: bỏ dấu câu, viết thường
        # Mẹo: Chuyển các ký tự đặc biệt như '/' thành khoảng trắng để khớp với cách AI đọc
        clean_sent = re.sub(r'[^\w\s]', ' ', sent).lower()
        sent_words = clean_sent.split()
        if len(sent_words) < 2: continue

        best_score = 0
        best_start_ts = None
        best_end_ts = None
        best_current_end_idx = last_token_idx

        # 4.2 Tìm ứng viên Start/End (Quét trong cửa sổ 200 tokens từ vị trí cũ)
        search_range = min(last_token_idx + 200, len(processed_tokens))
        
        # Thử các điểm bắt đầu khả thi (Anchor Start)
        for i in range(last_token_idx, min(last_token_idx + 50, len(processed_tokens))):
            # Nếu token khớp với 1 trong 2 từ đầu của câu
            if any(word in processed_tokens[i] for word in sent_words[:2]):
                current_start_ts = processed_timestamps[i]
                
                # Tìm điểm kết thúc khả thi (Anchor End)
                # Giới hạn tìm kiếm dựa trên độ dài ước tính của câu (ít nhất 0.2s mỗi từ)
                min_tokens_to_skip = int(len(sent_words) * 0.6)
                for j in range(i + min_tokens_to_skip, min(i + 150, len(processed_tokens))):
                    if any(word in processed_tokens[j] for word in sent_words[-2:]):
                        current_end_ts = processed_timestamps[j]
                        
                        # 4.3 THẨM ĐỊNH TOÀN BỘ (So khớp mờ đoạn ruột)
                        # Đây là bước "so toàn bộ" mà bạn muốn
                        candidate_tokens = " ".join(processed_tokens[i : j+1])
                        score = fuzz.ratio(clean_sent, candidate_text := candidate_tokens)
                        
                        if score > best_score:
                            best_score = score
                            best_start_ts = current_start_ts
                            best_end_ts = current_end_ts
                            best_current_end_idx = j
                        
                        if score > 95: break # Nếu đã quá chuẩn thì dừng quét
                if best_score > 90: break

        # 4.4 FALLBACK (Nếu AI không khớp được anchor chuẩn)
        if best_score < 60:
            # Dùng mốc ngay sau câu trước
            best_start_ts = processed_timestamps[last_token_idx] if last_token_idx < len(processed_timestamps) else 0
            # Ước tính thời gian dựa trên tốc độ đọc tin tức (12 ký tự/giây)
            best_end_ts = best_start_ts + (len(sent) / 12.0)
            best_current_end_idx = last_token_idx + len(sent_words)
            print(f"⚠️ Câu {idx:02d}: Không khớp tốt (Score: {best_score:.1f}), dùng ước tính.")
        else:
            print(f"✅ Câu {idx:02d}: Khớp chuẩn (Score: {best_score:.1f}) | {best_start_ts:.2f}s -> {best_end_ts:.2f}s")

        # 4.5 CẬP NHẬT CHỐT CHẶN (Ngăn việc câu sau cắt trùng câu trước)
        last_token_idx = min(best_current_end_idx + 1, len(processed_tokens) - 1)

        # 4.6 CẮT AUDIO (Padding thêm 0.3s đầu, 0.5s cuối cho tự nhiên)
        start_ms = max(0, int((best_start_ts - 0.3) * 1000))
        end_ms = int((best_end_ts + 0.5) * 1000)
        
        chunk_audio = audio[start_ms:end_ms]
        if len(chunk_audio) > 500:
            chunk_name = f"{article_id}_{idx:03d}.wav"
            chunk_path = os.path.join(output_dir, chunk_name)
            chunk_audio.export(chunk_path, format="wav")

            metadata.append({
                "id": f"{article_id}_{idx:03d}",
                "text": sent.strip(),
                "audio_file": chunk_path,
                "score": round(best_score, 1),
                "duration": round(best_end_ts - best_start_ts, 2)
            })

    # Lưu metadata.jsonl
    with open(os.path.join(os.path.dirname(output_dir), "metadata.jsonl"), "a", encoding="utf-8") as f:
        for m in metadata:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")

    if os.path.exists(wav_16k): os.remove(wav_16k)
    return metadata