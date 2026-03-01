#!/bin/bash

# --- CẤU HÌNH MÀU SẮC LOG ---
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Hàm log có timestamp
log() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# --- 1. THIẾT LẬP ĐƯỜNG DẪN ---
# Di chuyển về thư mục gốc chứa folder video-counting (Giả định script nằm trong video-counting/scripts)
# cd "$(dirname "$0")/../.."  <-- Sửa lại cú pháp chuẩn
cd "$(dirname "$0")" || exit
cd ../.. || exit

log "Working Directory: $(pwd)"

# --- 2. HÀM DỌN DẸP (CLEANUP) ---
# Hàm này sẽ chạy khi bạn nhấn Ctrl+C
cleanup() {
    echo ""
    log "Đang dừng pipeline..."
    
    # Kill các tiếng trình Python chạy nền nếu đã lưu PID
    if [ -n "$PRODUCER_PID" ]; then kill "$PRODUCER_PID"; fi
    if [ -n "$CONSUMER_PID" ]; then kill "$CONSUMER_PID"; fi
    
    # Tùy chọn: Tắt docker nếu muốn
    # docker-compose down
    
    success "Đã dừng toàn bộ hệ thống!"
    exit
}
# Bắt tín hiệu Ctrl+C (SIGINT)
trap cleanup SIGINT

# --- 3. KHỞI ĐỘNG KAFKA ---
log "Starting Kafka..."
docker-compose up -d

# Chờ Kafka khởi động (Quan trọng)
log "Waiting 10s for Kafka to be ready..."
sleep 10 
success "Kafka is ready!"

# --- 4. KÍCH HOẠT VENV ---
if [ -f "video-counting/venv/Scripts/activate" ]; then
    source video-counting/venv/Scripts/activate
    success "Activated venv"
elif [ -f "venv/Scripts/activate" ]; then # Trường hợp thư mục khác
    source venv/Scripts/activate
    success "Activated venv (root)"
else
    error "Không tìm thấy môi trường ảo!"
    exit 1
fi

# --- 5. CHẠY PIPELINE SONG SONG (PARALLEL) ---

# A. Chạy Consumer trước (Best practice: để Consumer lắng nghe sẵn)
log "Starting Consumer..."
python "video-counting/src/processing/consumer.py" &
CONSUMER_PID=$!  # Lưu lại ID của process này
log "Consumer started with PID: $CONSUMER_PID"

sleep 15  # Đợi Consumer sẵn sàng

# B. Chạy Dashboard (Streamlit)
log "Starting Dashboard..."
# Streamlit thường in ra nhiều log, có thể muốn ẩn bớt log hệ thống
streamlit run "video-counting/src/dashboard/main.py" --server.headless true &
DASHBOARD_PID=$!
log "Dashboard started with PID: $DASHBOARD_PID"

# C. Chạy Producer (Cuối cùng để bắt đầu đẩy data)
log "Starting Producer..."
python "video-counting/src/producer/producer.py" \
    --input "video-counting/data/traffic_2.mp4" \
    --cam_id CAM_HAN_02 &
PRODUCER_PID=$!
log "Producer started with PID: $PRODUCER_PID"

success "PIPELINE IS RUNNING! (Press Ctrl+C to stop)"

# --- 6. GIỮ SCRIPT HOẠT ĐỘNG ---
# Dùng lệnh wait để giữ terminal không bị thoát, chờ các tiến trình con
wait