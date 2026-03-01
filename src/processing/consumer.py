import sys
import logging
import base64
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf
from typing import Iterator
import os

# --- CẤU HÌNH MÔI TRƯỜNG ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SparkVehicleCounter")

class AppConfig:
    KAFKA_BOOTSTRAP = "kafka:29092" # Lưu ý: Dùng tên service trong docker-compose, không dùng localhost
    KAFKA_TOPIC = "vehicle_stream"
    PG_URL = "jdbc:postgresql://host.docker.internal:5432/traffic_db" 
    PG_TABLE = "vehicle_counts"
    PG_USER = "postgres"
    PG_PASS = "1234"
    PG_DRIVER = "org.postgresql.Driver"
    
    # SỬA ĐƯỜNG DẪN LINUX (Đảm bảo bạn đã mount volume này trong docker-compose)
    MODEL_PATH = "/app/models/yolo11n.onnx" 
    CHECKPOINT_DIR = "/tmp/checkpoints/vehicle-counting"
# ==========================================
# UDF ĐƯỢC VIẾT LẠI (SỬ DỤNG OPENCV DNN)
# ==========================================
@pandas_udf(IntegerType())
def count_vehicles_udf(frame_series_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    UDF xử lý ảnh theo batch (Series) sử dụng OpenCV DNN để đọc ONNX..
    """
    import cv2
    import numpy as np
    import sys

    # --- KHỞI TẠO MODEL 1 LẦN CHO MỖI WORKER ---
    # Kiểm tra xem model đã load vào biến tĩnh của hàm chưa để tránh load lại nhiều lần
    if not hasattr(count_vehicles_udf, "_net"):
        try:
            # Load ONNX bằng OpenCV (Cực nhẹ)
            net = cv2.dnn.readNetFromONNX(AppConfig.MODEL_PATH)
            net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
            net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
            count_vehicles_udf._net = net
            print("UDF Worker: Loaded ONNX model successfully", file=sys.stderr)
        except Exception as e:
            print(f"UDF Worker Error: Failed to load ONNX - {e}", file=sys.stderr)
            count_vehicles_udf._net = None

    net = count_vehicles_udf._net
    
    # Cấu hình tham số YOLO
    INPUT_SIZE = 640
    CONF_THRESHOLD = 0.5
    NMS_THRESHOLD = 0.4
    TARGET_CLASSES = [2, 3, 5, 7] # Car, Motor, Bus, Truck

    # --- XỬ LÝ DỮ LIỆU ---
    for frame_series in frame_series_iter:
        results_list = []
        
        for frame_b64 in frame_series:
            cnt = 0
            if net is None or not frame_b64 or len(frame_b64) < 10:
                results_list.append(0)
                continue
                
            try:
                # 1. Decode Base64 -> Image
                img_bytes = base64.b64decode(frame_b64)
                nparr = np.frombuffer(img_bytes, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

                if img is not None:
                    # 2. Preprocess (Chuẩn hóa ảnh cho YOLO)
                    blob = cv2.dnn.blobFromImage(img, 1/255.0, (INPUT_SIZE, INPUT_SIZE), swapRB=True, crop=False)
                    net.setInput(blob)
                    
                    # 3. Inference
                    outputs = net.forward()
                    
                    # 4. Post-processing (Giải mã output YOLOv8/v11)
                    # Output shape: (1, 84, 8400) -> Transpose thành (8400, 84)
                    outputs = np.array([cv2.transpose(outputs[0])])
                    rows = outputs.shape[1]
                    
                    boxes = []
                    scores = []
                    class_ids = []
                    
                    # Tính tỷ lệ scale để map lại box vào ảnh gốc
                    h_img, w_img = img.shape[:2]
                    x_factor = w_img / INPUT_SIZE
                    y_factor = h_img / INPUT_SIZE

                    # Lọc các box có độ tin cậy thấp
                    for i in range(rows):
                        classes_scores = outputs[0][i][4:]
                        _, max_score, _, (_, class_id) = cv2.minMaxLoc(classes_scores)
                        
                        if max_score >= CONF_THRESHOLD:
                            if class_id in TARGET_CLASSES:
                                box = outputs[0][i]
                                left = int((box[0] - 0.5 * box[2]) * x_factor)
                                top = int((box[1] - 0.5 * box[3]) * y_factor)
                                width = int(box[2] * x_factor)
                                height = int(box[3] * y_factor)
                                
                                boxes.append([left, top, width, height])
                                scores.append(float(max_score))
                                class_ids.append(class_id)
                    
                    # 5. NMS (Non-Maximum Suppression) để loại bỏ box trùng
                    if boxes:
                        indices = cv2.dnn.NMSBoxes(boxes, scores, CONF_THRESHOLD, NMS_THRESHOLD)
                        cnt = len(indices)
                        
                results_list.append(int(cnt))
                
            except Exception as e:
                results_list.append(0)

        yield pd.Series(results_list)

# ==========================================
# PIPELINE CLASS
# ==========================================
class VehicleCountingPipeline:
    def __init__(self):
        self.spark = self._init_spark()
        self.schema = StructType([
            StructField("cam_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("frame_id", LongType()),
            StructField("frame_data", StringType())
        ])

    def _init_spark(self):
        logger.info("Initializing Spark Session...")
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        postgres_package = "org.postgresql:postgresql:42.5.0"
        
        return SparkSession.builder \
            .appName("VehicleCounting_ONNX") \
            .config("spark.jars.packages", f"{kafka_package},{postgres_package}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20") \
            .getOrCreate()

    def read_stream(self):
        logger.info(f"Subscribing to Kafka topic: {AppConfig.KAFKA_TOPIC}")
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", AppConfig.KAFKA_BOOTSTRAP) \
            .option("subscribe", AppConfig.KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 20) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(F.from_json("value", self.schema).alias("data")) \
            .select("data.*")

    def process_stream(self, df_stream):
        # Gọi UDF
        return df_stream.withColumn("vehicle_count", count_vehicles_udf(F.col("frame_data"))) \
            .select("cam_id", F.col("timestamp").alias("stream_timestamp"), "vehicle_count")

    @staticmethod
    def _write_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty(): return
        logger.info(f"Processing Batch ID: {batch_id}")
        try:
            batch_df.write.mode("append").jdbc(
                AppConfig.PG_URL, AppConfig.PG_TABLE, 
                properties={"user": AppConfig.PG_USER, "password": AppConfig.PG_PASS, "driver": AppConfig.PG_DRIVER}
            )
            logger.info("Write success.")
        except Exception as e:
            logger.error(f"Write failed: {e}")

    def start(self):
        query = self.process_stream(self.read_stream()).writeStream \
            .foreachBatch(self._write_to_postgres) \
            .option("checkpointLocation", AppConfig.CHECKPOINT_DIR) \
            .start()
        query.awaitTermination()

if __name__ == "__main__":
    # Clean checkpoint cũ
    import shutil
    if os.path.exists(AppConfig.CHECKPOINT_DIR):
        try: shutil.rmtree(AppConfig.CHECKPOINT_DIR)
        except: pass
        
    VehicleCountingPipeline().start()   