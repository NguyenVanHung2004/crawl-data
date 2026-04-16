import os
import wave
import glob
import json
import requests
import numpy as np
import sherpa_onnx
import re
import tarfile
import shutil
import gc
from pydub import AudioSegment
from underthesea import sent_tokenize

# --- Cấu hình ---
# MODEL_DIR mặc định là thư mục models trong dự án
MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.getcwd(), "models"))

# Tự động tìm đường dẫn ffmpeg/ffprobe cho pydub trên môi trường Cloud
ffmpeg_find = shutil.which("ffmpeg")
ffprobe_find = shutil.which("ffprobe")
if ffmpeg_find:
    AudioSegment.converter = ffmpeg_find
if ffprobe_find:
    AudioSegment.ffprobe = ffprobe_find

def download_file(url, dest_path):
    """Hàm bổ trợ tải file có xử lý lỗi và headers."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        print(f"📥 Downloading {os.path.basename(dest_path)}...")
        resp = requests.get(url, headers=headers, stream=True, timeout=30)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536): # Tăng chunk size để tải nhanh hơn
                if chunk: f.write(chunk)
        print(f"✅ Downloaded {os.path.basename(dest_path)}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {os.path.basename(dest_path)}: {e}")
        if os.path.exists(dest_path): os.remove(dest_path)
        return False

def download_model_if_needed():
    """Tải model ZipFormer INT8 (Bản tối ưu cho Cloud) từ GitHub."""
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Check if INT8 models already exist
    if glob.glob(os.path.join(MODEL_DIR, "encoder-*.int8.onnx")):
        return

    # SỬ DỤNG BẢN INT8 ĐỂ TIẾT KIỆM RAM (Quan trọng cho Zeabur/Railway)
    model_url = "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-zipformer-vi-int8-2025-04-20.tar.bz2"
    archive_name = "model_vi_int8.tar.bz2"

    print(f"📂 Model check in: {MODEL_DIR}")
    print(f"📥 Downloading INT8 ASR Model (Cloud Optimized)...")
    
    if download_file(model_url, archive_name):
        print("📦 Extracting INT8 Zipformer...")
        try:
            with tarfile.open(archive_name, "r:bz2") as tar:
                tar.extractall(".")
            
            # Folder name updated to match the int8 release
            extracted_dir = "sherpa-onnx-zipformer-vi-int8-2025-04-20"
            if os.path.exists(extracted_dir):
                for f in os.listdir(extracted_dir):
                    src = os.path.join(extracted_dir, f)
                    dst = os.path.join(MODEL_DIR, f)
                    if os.path.exists(dst): os.remove(dst)
                    shutil.move(src, dst)
                shutil.rmtree(extracted_dir)
            print("✅ INT8 Model Ready")
        except Exception as e:
            print(f"❌ Extraction Failed: {e}")
        finally:
            if os.path.exists(archive_name): os.remove(archive_name)

def load_recognizer():
    try:
        download_model_if_needed()
    except Exception as e:
        print(f"⚠️ Model check warning: {e}")
    
    try:
        tokens = os.path.join(MODEL_DIR, "tokens.txt")
        # Tìm file bản INT8 ưu tiên
        encoder = glob.glob(os.path.join(MODEL_DIR, "encoder-*.int8.onnx")) or glob.glob(os.path.join(MODEL_DIR, "encoder-*.onnx"))
        decoder = glob.glob(os.path.join(MODEL_DIR, "decoder-*.int8.onnx")) or glob.glob(os.path.join(MODEL_DIR, "decoder-*.onnx"))
        joiner = glob.glob(os.path.join(MODEL_DIR, "joiner-*.int8.onnx")) or glob.glob(os.path.join(MODEL_DIR, "joiner-*.onnx"))
        
        if not (encoder and decoder and joiner):
            raise FileNotFoundError("Missing INT8 ONNX files.")

        print(f"⏳ [INIT] Loading Zipformer INT8 (Threads: 2)...")
        recognizer = sherpa_onnx.OfflineRecognizer.from_transducer(
            tokens=tokens,
            encoder=encoder[0],
            decoder=decoder[0],
            joiner=joiner[0],
            num_threads=2, # Giảm thread để không bị kill CPU
            sample_rate=16000,
            feature_dim=80,
        )
        return recognizer
    except Exception as e:
        print(f"❌ Recognizer Load Error: {e}")
        return None

def clean_token(t):
    return t.replace(' ', '').replace('▁', '').strip().lower()

def process_and_align(audio_path, text_path, output_dir, article_id, recognizer):
    # --- Bước 1: Decode Audio sang Waveform (Tối ưu RAM) ---
    print(f"🔈 Processing Audio: {os.path.basename(audio_path)}")
    
    # Thay vì dùng Pydub export rồi lại dùng wave open, ta dùng AudioSegment trực tiếp
    # sau đó gọi get_array_of_samples để tránh đọc từ disk 2 lần.
    audio_full = AudioSegment.from_file(audio_path).set_frame_rate(16000).set_channels(1)
    
    # Chuyển đổi sang numpy float32 ngay (giải phóng buffer int16 nhanh nhất có thể)
    samples = np.array(audio_full.get_array_of_samples()).astype(np.float32) / 32768.0
    
    # --- Bước 2: Nhận dạng lấy Timestamps (Tối ưu hóa Loop) ---
    chunk_size = 16000 * 30 # 30s chunks
    all_tokens = []
    all_timestamps = []
    offset = 0

    print(f"🎤 Running ASR (Zipformer INT8)...")
    for i in range(0, len(samples), chunk_size):
        chunk = samples[i : i + chunk_size]
        stream = recognizer.create_stream()
        stream.accept_waveform(16000, chunk)
        recognizer.decode_stream(stream)
        
        res = stream.result
        for t_idx, ts in enumerate(res.timestamps):
            all_tokens.append(clean_token(res.tokens[t_idx]))
            all_timestamps.append(ts + (offset / 16000))
        offset += len(chunk)

    # --- Bước 3: NLP & Alignment ---
    with open(text_path, 'r', encoding='utf-8') as f:
        standard_text = f.read()
    
    # Tách câu (Lưu ý: model underthesea sẽ được pre-download trong Dockerfile)
    sentences = sent_tokenize(standard_text)

    # Tiền xử lý tokens Zipformer (vẫn giữ logic gộp đánh vần)
    processed_tokens = []
    processed_timestamps = []
    temp_word = ""
    for k in range(len(all_tokens)):
        tk = all_tokens[k]
        if len(tk) == 1 and k < len(all_tokens) - 1:
            temp_word += tk
        else:
            processed_tokens.append(temp_word + tk)
            processed_timestamps.append(all_timestamps[k - len(temp_word)])
            temp_word = ""

    # Gióng hàng logic
    from rapidfuzz import fuzz
    os.makedirs(output_dir, exist_ok=True)
    metadata = []
    last_token_idx = 0 
    
    print(f"🎯 Aligning {len(sentences)} sentences...")
    for idx, sent in enumerate(sentences):
        clean_sent = re.sub(r'[^\w\s]', ' ', sent).lower()
        sent_words = clean_sent.split()
        if len(sent_words) < 2: continue

        best_score, best_start_ts, best_end_ts, best_end_idx = 0, 0, 0, last_token_idx

        # Cửa sổ tìm kiếm
        search_limit = min(last_token_idx + 250, len(processed_tokens))
        for i in range(last_token_idx, min(last_token_idx + 60, len(processed_tokens))):
            if any(w in processed_tokens[i] for w in sent_words[:2]):
                min_skip = int(len(sent_words) * 0.55)
                for j in range(i + min_skip, min(i + 180, len(processed_tokens))):
                    if any(w in processed_tokens[j] for w in sent_words[-2:]):
                        score = fuzz.ratio(clean_sent, " ".join(processed_tokens[i:j+1]))
                        if score > best_score:
                            best_score, best_start_ts, best_end_ts, best_end_idx = score, processed_timestamps[i], processed_timestamps[j], j
                        if score > 94: break
                if best_score > 85: break

        if best_score < 55:
            # Fallback (Uoc tinh thoi gian dua tren do dai van ban)
            best_start_ts = processed_timestamps[last_token_idx] if last_token_idx < len(processed_timestamps) else 0
            best_end_ts = best_start_ts + (len(sent) / 12.5) 
            best_end_idx = last_token_idx + len(sent_words)

        last_token_idx = min(best_end_idx + 1, len(processed_tokens) - 1)

        # Cắt và lưu
        ps, pe = max(0, int((best_start_ts - 0.25) * 1000)), int((best_end_ts + 0.5) * 1000)
        chunk_seg = audio_full[ps:pe]
        if len(chunk_seg) > 400:
            nm = f"{article_id}_{idx:03d}.wav"
            pth = os.path.join(output_dir, nm)
            chunk_seg.export(pth, format="wav")
            metadata.append({
                "id": f"{article_id}_{idx:03d}",
                "text": sent.strip(),
                "audio_file": pth,
                "score": round(best_score, 1),
                "duration": round((pe-ps)/1000, 2)
            })

    # Flush metadata & Clean Up
    with open(os.path.join(os.path.dirname(output_dir), "metadata.jsonl"), "a", encoding="utf-8") as f:
        for m in metadata:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")

    # Giai phong memory TRIET DE
    del samples
    del audio_full
    gc.collect() 
    return metadata