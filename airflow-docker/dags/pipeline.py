"""
Airflow DAG for Vehicle Counting Pipeline
Orchestrates Kafka producer and Spark consumer for real-time vehicle counting
Updated for reliability and correct path handling.
"""
import os
import socket
import logging
import subprocess
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# --- CONFIGURATION ---
# Định nghĩa đường dẫn gốc tuyệt đối để tránh lỗi "File not found"
PROJECT_ROOT = "/opt/airflow/project"
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SRC_DIR = os.path.join(PROJECT_ROOT, "src")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'vehicle_counting_pipeline',
    default_args=default_args,
    description='Real-time vehicle counting using Kafka and Spark',
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=['kafka', 'spark', 'yolo', 'vehicle-counting'],
)

def check_kafka_health(**context):
    """Check if Kafka is running and accessible"""
    kafka_host = 'host.docker.internal' # Lưu ý: Chỉ hoạt động tốt trên Docker Desktop
    kafka_port = 9092
    
    logging.info(f"Checking Kafka connection at {kafka_host}:{kafka_port}")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((kafka_host, kafka_port))
        sock.close()
        
        if result == 0:
            logging.info(f"SUCCESS: Kafka is accessible at {kafka_host}:{kafka_port}")
            return True
        else:
            raise Exception(f"FAILURE: Cannot connect to Kafka at {kafka_host}:{kafka_port} (Error code: {result})")
    except Exception as e:
        logging.error(f"Kafka health check error: {e}")
        raise

def check_postgres_health(**context):
    """Check if PostgreSQL is running and accessible"""
    import psycopg2
    
    db_config = {
        'host': 'host.docker.internal',
        'port': 5432,
        'database': 'traffic_db',
        'user': 'postgres',
        'password': '1234'
    }
    
    try:
        logging.info(f"Checking Postgres connection at {db_config['host']}")
        conn = psycopg2.connect(**db_config)
        conn.close()
        logging.info("SUCCESS: PostgreSQL is accessible")
        return True
    except Exception as e:
        logging.error(f"PostgreSQL health check failed: {e}")
        raise

# def start_kafka_producer(**context):
#     """Starts the Kafka producer as a background process"""
    
#     video_path = os.path.join(DATA_DIR, "traffic_1.mp4")
#     producer_script = os.path.join(SRC_DIR, "producer", "producer.py")
#     cam_id = "CAM_HAN_01"
#     fps = 10

#     # Kiểm tra file tồn tại trước khi chạy
#     if not os.path.exists(producer_script):
#         raise FileNotFoundError(f"Producer script not found at: {producer_script}")
#     if not os.path.exists(video_path):
#         raise FileNotFoundError(f"Video file not found at: {video_path}")

#     # File log cho producer
#     log_file = open(os.path.join(log_dir, f"producer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), "w")

#     logging.info(f"Writing Producer logs to: {log_file.name}")

#     cmd = [
#         "python",
#         producer_script,
#         "--input", video_path,
#         "--cam_id", cam_id,
#         "--fps", str(fps)
#     ]

#     logging.info(f"Executing command: {' '.join(cmd)}")

#     # Set PYTHONPATH để script import được các module trong project
#     env = os.environ.copy()
#     env["PYTHONPATH"] = f"{PROJECT_ROOT}:{env.get('PYTHONPATH', '')}"

#     # 🔥 CHẠY BACKGROUND (start_new_session=True để tách process khỏi Airflow worker)
#     process = subprocess.Popen(
#         cmd,
#         stdout=log_file,  # 🔥 Ghi log ra file
#         stderr=log_file,  # 🔥 Ghi cả lỗi ra file này
#         start_new_session=True,
#         env=env
#     )

#     # Metadata config để task Monitor đọc được
#     producer_config = {
#         "start_time": datetime.now().isoformat(),
#         "script": producer_script,
#         "pid": process.pid,
#         "cam_id": cam_id,
#         "video": video_path
#     }

#     # Push cả PID và Config
#     context["task_instance"].xcom_push(key="producer_pid", value=process.pid)
#     context["task_instance"].xcom_push(key="producer_config", value=producer_config)

#     logging.info(f"Producer started successfully with PID {process.pid}")
#     return process.pid

# def start_spark_consumer(**context):
#     """Starts the Spark consumer script"""
    
#     # Sửa đường dẫn tuyệt đối
#     consumer_script = os.path.join(SRC_DIR, "processing", "consumer.py")

#     if not os.path.exists(consumer_script):
#         raise FileNotFoundError(f"Consumer script not found at: {consumer_script}")

#     logging.info(f"Starting Spark consumer script: {consumer_script}")
#     # File log cho consumer
#     log_file = open(os.path.join(log_dir, f"consumer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), "w")

#     logging.info(f"Writing Consumer logs to: {log_file.name}")

#     cmd = ["python", consumer_script]
    
#     # Set PYTHONPATH
#     env = os.environ.copy()
#     env["PYTHONPATH"] = f"{PROJECT_ROOT}:{env.get('PYTHONPATH', '')}"

#     # ⚠️ Chạy background detached
#     process = subprocess.Popen(
#         cmd,
#         stdout=log_file,  # 🔥 Ghi log ra file
#         stderr=log_file,  # 🔥 Ghi cả lỗi ra file này
#         start_new_session=True,
#         env=env
#     )

#     consumer_config = {
#         "start_time": datetime.now().isoformat(),
#         "consumer_script": consumer_script,
#         "pid": process.pid
#     }

#     context["task_instance"].xcom_push(key="consumer_pid", value=process.pid)
#     context["task_instance"].xcom_push(key="consumer_config", value=consumer_config)

#     logging.info(f"Spark consumer started with PID={process.pid}")
#     return f"Started PID {process.pid}"

# def monitor_pipeline(**context):
#     """Log the status of the launched processes"""
    
#     # Lấy thông tin từ XCom
#     ti = context['task_instance']
#     producer_config = ti.xcom_pull(task_ids='start_producer', key='producer_config')
#     consumer_config = ti.xcom_pull(task_ids='start_consumer', key='consumer_config')
    
#     logging.info("=== PIPELINE MONITORING REPORT ===")
    
#     if producer_config:
#         logging.info(f"✅ Producer running (PID: {producer_config.get('pid')})")
#         logging.info(f"   - Input: {producer_config.get('video')}")
#         logging.info(f"   - Started: {producer_config.get('start_time')}")
#     else:
#         logging.error("❌ Could not retrieve Producer configuration!")

#     if consumer_config:
#         logging.info(f"✅ Consumer running (PID: {consumer_config.get('pid')})")
#         logging.info(f"   - Script: {consumer_config.get('consumer_script')}")
#     else:
#         logging.error("❌ Could not retrieve Consumer configuration!")
    
#     return "Monitoring check complete."

# --- TASK DEFINITIONS ---

task_check_kafka = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

task_check_postgres = PythonOperator(
    task_id='check_postgres_health',
    python_callable=check_postgres_health,
    dag=dag,
)

# task_start_producer = PythonOperator(
#     task_id='start_producer',
#     python_callable=start_kafka_producer,
#     dag=dag,
# )

# task_start_consumer = PythonOperator(
#     task_id='start_consumer',
#     python_callable=start_spark_consumer,
#     dag=dag,
# )

# task_monitor = PythonOperator(
#     task_id='monitor_pipeline',
#     python_callable=monitor_pipeline,
#     dag=dag,
# )

def run_pipeline_src1(**context):
    """Placeholder for running the entire pipeline from source 1"""
    logging.info("Pipeline execution placeholder")
    # Here you would implement the actual pipeline logic
    return "Pipeline executed"
    
task_run_pipeline_src1 = PythonOperator(
    task_id='run_pipeline',
    python_callable=lambda: logging.info("Pipeline execution placeholder"),
    dag=dag,
)

# --- DEPENDENCIES ---
[task_check_kafka, task_check_postgres] >> task_run_pipeline_src1