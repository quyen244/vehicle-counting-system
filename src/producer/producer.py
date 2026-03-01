import cv2
import json
import time
import base64
import sys
import logging
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime

# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class VideoKafkaProducer:
    def __init__(self, bootstrap_servers, topic, cam_id, video_path, target_fps=10, resize_dim=(640, 640)):
        """
        Khởi tạo Video Kafka Producer.
        :param bootstrap_servers: Địa chỉ Kafka Server (vd: localhost:9092)
        :param topic: Tên Kafka Topic
        :param cam_id: ID định danh cho Camera
        :param video_path: Đường dẫn file video hoặc index camera (0, 1)
        :param target_fps: Tốc độ gửi tin (frames per second) mong muốn để tránh nghẽn mạng
        :param resize_dim: Kích thước resize ảnh (width, height) tối ưu cho Model
        """
        self.topic = topic
        self.cam_id = cam_id
        self.video_path = video_path
        self.target_fps = target_fps
        self.resize_dim = resize_dim
        self.sleep_interval = 1.0 / target_fps
        
        # Khởi tạo Kafka Producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Tăng timeout và kích thước batch nếu mạng chậm
                request_timeout_ms=20000,
                retries=5
            )
            logging.info(f"Connected to Kafka at {bootstrap_servers}")
        except Exception as e:
            logging.error(f"Failed to connect to Kafka: {e}")
            sys.exit(1)

    def on_send_success(self, record_metadata):
        """Hàm này được gọi khi gửi THÀNH CÔNG"""
        # Logging mức debug để đỡ rác màn hình, chỉ bật khi cần
        logging.debug(f"Sent to topic: {record_metadata.topic} partition: {record_metadata.partition}")
        pass

    def on_send_error(self, excp):
        """Hàm này được gọi khi gửi THẤT BẠI"""
        logging.error(f"I am an errback: {excp}")

    def start_streaming(self):
        """Bắt đầu đọc video và gửi stream"""
        # Xử lý input: Nếu là số (vd: '0') thì convert sang int để mở webcam, ngược lại là path file
        src = int(self.video_path) if self.video_path.isdigit() else self.video_path

        try:
            cap = cv2.VideoCapture(src)
        except Exception as e:
            logging.info('error in reading video' , e)
        
        if not cap.isOpened():
            logging.error(f"Could not open video source: {self.video_path}")
            return

        logging.info(f"Started streaming from {self.cam_id} to topic '{self.topic}'...")
        
        frame_count = 0
        try:
            while True:
                start_time = time.time()
                
                ret, frame = cap.read()
                if not ret:
                    logging.info(f"Sent {frame_count} frames...")
                    logging.info("End of video stream.")
                    break

                # 1. Resize: Quan trọng để giảm tải băng thông và phù hợp input model (VD: YOLOv8 640x640)
                frame = cv2.resize(frame, self.resize_dim)

                # 2. Encode JPEG: Nén ảnh trước khi gửi
                _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
                
                # 3. Base64 Encode
                frame_base64 = base64.b64encode(buffer).decode('utf-8')

                # 4. Tạo Payload
                current_dt = datetime.datetime.now()
                formatted_timestamp = current_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                message = {
                    'cam_id': self.cam_id,
                    'timestamp': formatted_timestamp,
                    'frame_id': frame_count,
                    'frame_data': frame_base64
                }

                # 5. Gửi Kafka (Asynchronous)
                # Đăng ký callback đúng cách cho kafka-python
                future = self.producer.send(self.topic, value=message)
                future.add_callback(self.on_send_success) # Gắn hàm xử lý thành công
                future.add_errback(self.on_send_error)
                
                frame_count += 1
                if frame_count % 50 == 0:
                    logging.info(f"Sent {frame_count} frames...")

                # 6. Rate Limiting: Điều chỉnh tốc độ gửi
                process_time = time.time() - start_time
                time_to_wait = self.sleep_interval - process_time
                if time_to_wait > 0:
                    time.sleep(time_to_wait)

        except KeyboardInterrupt:
            logging.info("Streaming stopped by user.")
        except Exception as e:
            logging.error(f"Error during streaming: {e}")
        finally:
            cap.release()
            self.producer.flush()
            self.producer.close()
            logging.info("Resources released.")

# Hàm parse arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Video Stream Producer")
    
    parser.add_argument('--input', type=str, required=True, 
                        help='Path to video file or camera index (e.g., 0, video.mp4)')
    parser.add_argument('--cam_id', type=str, required=True, 
                        help='Unique Camera ID (e.g., CAM_01)')
    parser.add_argument('--topic', type=str, default='vehicle_stream', 
                        help='Kafka topic name (default: vehicle_stream)')
    parser.add_argument('--server', type=str, default='localhost:9092', 
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--fps', type=int, default=10, 
                        help='Target FPS to stream (default: 10)')
    
    return parser.parse_args()

if __name__ == "__main__":
    # 1. Lấy tham số từ dòng lệnh
    args = parse_args()
    
    # 2. Khởi tạo Producer Object
    video_producer = VideoKafkaProducer(
        bootstrap_servers=args.server,
        topic=args.topic,
        cam_id=args.cam_id,
        video_path=args.input,
        target_fps=args.fps
    )
    
    # 3. Chạy
    video_producer.start_streaming()


    #python3 producer.py --input ../../data/traffic_1.mp4 --cam_id CAM_HAN_01 --server kafka:29092

    #python producer.py --input 0 --cam_id CAM_LIVE_01 --server 192.168.1.10:9092 thay đổi server 

    # python producer.py --input data/videos/sample.mp4 --cam_id CAM_TEST --fps 5 giảm tốc độ
    # python producer.py --input ../../data/traffic_2.mp4 --cam_id CAM_HAN_02