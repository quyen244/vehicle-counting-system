# ABSA Big Data Streaming System

Hệ thống ABSA (Aspect-Based Sentiment Analysis) theo thời gian thực sử dụng Kafka, Spark Structured Streaming, Airflow, PostgreSQL, và Streamlit.

## 📋 Mục Tiêu

- ✅ Xử lý dữ liệu bình luận real-time qua Kafka & Spark Streaming
- ✅ Áp dụng mô hình ABSA để phân tích 8 khía cạnh cảm xúc
- ✅ Lưu kết quả vào PostgreSQL
- ✅ Hiển thị dashboard real-time (Streamlit)
- ✅ Pipeline tự động huấn luyện và cập nhật mô hình (Airflow)
- ✅ Đánh giá và deploy mô hình mới tự động

## 🏗️ Kiến Trúc Hệ Thống

```
CSV Data → Kafka Producer → Kafka Topic → Spark Consumer → PostgreSQL
                                                ↓
                                          ABSA Model Inference
                                                ↓
                                          Streamlit Dashboard
                                          
Airflow Scheduler → Train Model → Evaluate → Deploy (if better)
```

## 📁 Cấu Trúc Thư Mục

```
video-counting/
├── src2/
│   ├── config.py              # Cấu hình tập trung
│   ├── producer.py            # Kafka Producer
│   ├── consumer.py            # Spark Streaming Consumer
│   ├── dashboard.py           # Streamlit Dashboard
│   ├── train_model.py         # Script huấn luyện mô hình
│   └── evaluate_model.py      # Script đánh giá mô hình
├── sql/
│   └── init_db.sql            # Schema PostgreSQL
├── models/
│   ├── vietnamese_sentiment_model (1).keras  # Mô hình production
│   └── candidates/            # Mô hình candidate
├── archive/
│   ├── test_data.csv          # Dữ liệu test
│   └── train_data.csv         # Dữ liệu train
└── airflow-docker/
    └── dags/
        └── absa_pipeline_dag.py  # Airflow DAG
```

## 🔧 Yêu Cầu Hệ Thống

### Software Requirements
- Python 3.8+
- Apache Kafka
- Apache Spark 3.5.0
- PostgreSQL 12+
- Apache Airflow 2.0+

### Python Dependencies
```bash
pip install pandas numpy tensorflow keras
pip install pyspark==3.5.0
pip install kafka-python
pip install psycopg2-binary
pip install streamlit plotly
pip install scikit-learn
pip install apache-airflow
```

## 🚀 Hướng Dẫn Cài Đặt

### 1. Thiết Lập PostgreSQL

```bash
# Tạo database
psql -U postgres
CREATE DATABASE absa_db;
\c absa_db

# Chạy script khởi tạo schema
psql -U postgres -d absa_db -f sql/init_db.sql
```

### 2. Khởi Động Kafka

```bash
# Start Zookeeper
zookeeper-server-start.bat config/zookeeper.properties

# Start Kafka Server
kafka-server-start.bat config/server.properties

# Tạo topic
kafka-topics.bat --create --topic absa_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. Cấu Hình

Chỉnh sửa `src2/config.py` theo môi trường của bạn:
- Kafka bootstrap servers
- PostgreSQL connection
- Model paths
- Data paths

## 📊 Sử Dụng Hệ Thống

### Bước 1: Khởi Động Producer

```bash
cd src2
python producer.py --topic absa_stream --server localhost:9092 --data ../archive/test_data.csv --rate 10
```

**Tham số:**
- `--topic`: Tên Kafka topic (mặc định: absa_stream)
- `--server`: Kafka bootstrap server (mặc định: localhost:9092)
- `--data`: Đường dẫn file CSV
- `--rate`: Số message/giây (mặc định: 10)

### Bước 2: Khởi Động Consumer (Spark Streaming)

```bash
cd src2
python consumer.py
```

Consumer sẽ:
- Đọc messages từ Kafka
- Áp dụng mô hình ABSA
- Lưu kết quả vào PostgreSQL

### Bước 3: Khởi Động Dashboard

```bash
cd src2
streamlit run dashboard.py
```

Dashboard sẽ khả dụng tại: `http://localhost:8501`

**Features:**
- Thống kê tổng quan
- Biểu đồ xu hướng theo thời gian
- Phân bố sentiment cho từng aspect
- Bảng reviews gần đây
- Auto-refresh

## 🤖 Model Retraining Pipeline (Airflow)

### Thiết Lập Airflow

```bash
cd airflow-docker
# Khởi động Airflow containers
docker-compose up -d

# Truy cập Airflow UI: http://localhost:8080
# username: airflow, password: airflow
```

### Airflow DAG: `absa_model_retraining`

**Schedule**: Mỗi Chủ Nhật lúc 2 AM

**Tasks:**
1. **check_data_availability** - Kiểm tra dữ liệu train/test
2. **train_new_model** - Huấn luyện mô hình mới
3. **get_latest_candidate** - Lấy mô hình candidate mới nhất
4. **evaluate_and_deploy** - Đánh giá và deploy nếu tốt hơn
5. **send_notification** - Gửi thông báo

### Manual Training

```bash
# Huấn luyện mô hình mới
cd src2
python train_model.py --data ../archive/train_data.csv --epochs 10 --batch-size 32

# Đánh giá mô hình
python evaluate_model.py --candidate ../models/candidates/model_v_XXXXXX.keras --auto-deploy
```

## 📈 Dashboard Features

### Metrics Hiển Thị

1. **Overall Stats** (1 giờ gần nhất)
   - Tổng số reviews
   - Số reviews Positive/Negative/Neutral

2. **Sentiment Trends**
   - Xu hướng sentiment theo thời gian
   - Các aspect chính: Price, Quality, General

3. **All Aspects Comparison**
   - So sánh sentiment giữa 8 aspects
   - Biểu đồ grouped bar chart

4. **Individual Aspect Distribution**
   - Pie charts cho từng aspect
   - 8 aspects: Price, Shipping, Outlook, Quality, Size, Shop_Service, General, Others

5. **Recent Reviews Table**
   - 20 reviews gần nhất
   - Predictions cho tất cả aspects
   - Timestamp

## 🔍 Model Details

### Architecture
- **Input**: Vietnamese text reviews
- **Preprocessing**: Stopword removal, teencode normalization
- **Embedding**: TextVectorization + Embedding layer
- **Encoder**: Bidirectional LSTM (128, 64 units)
- **Output**: 8 classification heads (multi-output)

### Labels
- **-1**: None (không đề cập)
- **0**: Negative (tiêu cực)
- **1**: Positive (tích cực)
- **2**: Neutral (trung lập)

### Aspects
1. Price - Giá cả
2. Shipping - Vận chuyển
3. Outlook - Ngoại hình
4. Quality - Chất lượng
5. Size - Kích thước
6. Shop_Service - Dịch vụ
7. General - Tổng quan
8. Others - Khác

## 🗄️ Database Schema

### Table: `sentiment_analysis`
Lưu kết quả phân tích streaming:
- `id`: Primary key
- `review`: Text review
- `pred_*`: Predictions cho 8 aspects
- `actual_*`: Ground truth (nếu có)
- `processed_at`: Timestamp

### Table: `model_versions`
Quản lý versions mô hình:
- `version`: Version name
- `model_path`: Đường dẫn mô hình
- `accuracy_*`, `f1_score_*`: Metrics cho từng aspect
- `is_production`: Flag production model
- `deployed_at`: Timestamp deploy

## 📝 Workflow

### Streaming Pipeline
1. Producer đọc CSV → gửi messages đến Kafka
2. Consumer nhận messages → preprocess text
3. Apply ABSA model → predict 8 aspects
4. Lưu kết quả vào PostgreSQL
5. Dashboard query PostgreSQL → hiển thị real-time

### Retraining Pipeline
1. Airflow trigger DAG (weekly/manual)
2. Load training data
3. Train new model với Bi-LSTM architecture
4. Evaluate trên test set
5. Compare với production model
6. Deploy nếu tốt hơn (accuracy + F1-score)
7. Update database & copy model file

## 🛠️ Troubleshooting

### Kafka Connection Issues
```bash
# Kiểm tra Kafka đang chạy
kafka-topics.bat --list --bootstrap-server localhost:9092

# Xem messages trong topic
kafka-console-consumer.bat --topic absa_stream --bootstrap-server localhost:9092 --from-beginning
```

### PostgreSQL Connection Issues
```bash
# Kiểm tra PostgreSQL đang chạy
psql -U postgres -c "SELECT version();"

# Kiểm tra database
psql -U postgres -d absa_db -c "\dt"
```

### Spark Issues
- Đảm bảo Java 8 hoặc 11 được cài đặt
- Kiểm tra JAVA_HOME environment variable
- Download PostgreSQL JDBC driver nếu cần

## 📊 Testing

### Test Producer
```bash
python producer.py --rate 5  # Gửi 5 messages/giây
```

### Test Consumer
```bash
# Kiểm tra logs
# Spark sẽ log thông tin về batch processing
```
docker compose run --rm airflow-webserver rm -f /opt/airflow/airflow-webserver.pid
### Test Database
```sql
-- Kiểm tra số records
SELECT COUNT(*) FROM sentiment_analysis;

-- Xem recent data
SELECT * FROM sentiment_analysis ORDER BY processed_at DESC LIMIT 10;

-- Sentiment distribution
SELECT pred_general, COUNT(*) FROM sentiment_analysis GROUP BY pred_general;
```

### Test Dashboard
- Truy cập `http://localhost:8501`
- Bật auto-refresh
- Kiểm tra các charts cập nhật real-time

## 🎯 Performance Tips

1. **Kafka**: Tăng partitions cho higher throughput
2. **Spark**: Điều chỉnh `maxOffsetsPerTrigger` và `processingTime`
3. **PostgreSQL**: Thêm indexes cho các queries thường dùng
4. **Model**: Sử dụng GPU cho inference nhanh hơn

## 📚 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [TensorFlow/Keras](https://www.tensorflow.org/)
- [Streamlit](https://docs.streamlit.io/)
- [Apache Airflow](https://airflow.apache.org/docs/)

## 👥 Contributors

ABSA Team

## 📄 License

Internal Project
