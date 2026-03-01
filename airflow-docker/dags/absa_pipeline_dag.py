"""
Airflow DAG for ABSA Model Retraining Pipeline (SSH Version)
Automatically trains, evaluates, and deploys new models on separate TensorFlow Container
"""

import logging
import socket
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago

# --- CONFIGURATION ---
# Thông tin kết nối tới container TensorFlow (Model Trainer)
TRAINER_HOST = 'model-trainer'  # Tên service trong docker-compose
TRAINER_USER = 'root'           # User đã cấu hình trong Dockerfile.trainer
TRAINER_PASSWORD = 'root123'    # Pass đã cấu hình trong Dockerfile.trainer
SSH_PORT = 22

# Đường dẫn TRONG container Model Trainer (phải khớp với volume mount)
REMOTE_PROJECT_DIR = "/opt/project"
REMOTE_SRC_DIR = f"{REMOTE_PROJECT_DIR}/src2"
TRAIN_SCRIPT = f"{REMOTE_SRC_DIR}/train_model.py"
EVAL_SCRIPT = f"{REMOTE_SRC_DIR}/evaluate_model.py"
TRAIN_DATA = f"{REMOTE_PROJECT_DIR}/archive/train_data.csv"
TEST_DATA = f"{REMOTE_PROJECT_DIR}/archive/test_data.csv"
CANDIDATE_DIR = f"{REMOTE_PROJECT_DIR}/models/candidates"

# Default arguments
default_args = {
    'owner': 'absa_team',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'absa_model_retraining',
    default_args=default_args,
    description='Automated ABSA model retraining via SSH (Isolated)',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['absa', 'ml', 'ssh', 'tensorflow'],
)

# --- TẠO HOOK KẾT NỐI SSH ---
ssh_hook = SSHHook(
    remote_host=TRAINER_HOST,
    username=TRAINER_USER,
    password=TRAINER_PASSWORD,
    port=SSH_PORT,
    keepalive_interval=10,
    cmd_timeout=3600 # Tăng timeout cho việc training
)

# --- HEALTH CHECKS ---
def check_trainer_connectivity():
    """Kiểm tra xem container Model Trainer có online và mở port 22 không"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((TRAINER_HOST, SSH_PORT))
        sock.close()
        if result == 0:
            logging.info(f"SUCCESS: {TRAINER_HOST} is accessible on port {SSH_PORT}")
            return True
        raise Exception(f"Cannot connect to {TRAINER_HOST}:{SSH_PORT}")
    except Exception as e:
        logging.error(str(e))
        raise

# --- COMMANDS (Logic chạy trên container Model Trainer) ---

# 1. Command Check Data: Chạy python script nhỏ để kiểm tra file tồn tại
cmd_check_data = f"""
echo "Checking data availability..."
python3 -c "
import os, sys, pandas as pd
try:
    if not os.path.exists('{TRAIN_DATA}'): raise FileNotFoundError('{TRAIN_DATA}')
    if not os.path.exists('{TEST_DATA}'): raise FileNotFoundError('{TEST_DATA}')
    
    # Quick CSV check
    df = pd.read_csv('{TRAIN_DATA}')
    print(f'Data OK. Training samples: {{len(df)}}')
except Exception as e:
    print(str(e))
    sys.exit(1)
"
"""

# 2. Command Train Model: Chạy script training chính
cmd_train_model = f"""
export PYTHONPATH={REMOTE_SRC_DIR}:$PYTHONPATH
echo "Starting training..."
python3 "{TRAIN_SCRIPT}" --data "{TRAIN_DATA}" --epochs 1 --batch-size 32
"""

# 3. Command Find Candidate: Tìm file model mới nhất và in đường dẫn ra stdout (để XCom bắt lấy)
cmd_find_candidate = f"""
python3 -c "
import glob, os, sys
files = glob.glob('{CANDIDATE_DIR}/model_v_*.keras') + glob.glob('{CANDIDATE_DIR}/model_v_*.h5')
if not files: 
    print('No model found')
    sys.exit(1)
latest = max(files, key=os.path.getmtime)
print(latest, end='')  # In ra khong xuong dong de XCom lay chinh xac
"
"""

# 4. Command Evaluate: Lấy path từ task trước để chạy đánh giá
# Lưu ý: {{ ... }} là cú pháp Jinja của Airflow để lấy XCom
cmd_evaluate = f"""
export PYTHONPATH={REMOTE_SRC_DIR}:$PYTHONPATH
MODEL_PATH=$(echo "{{{{ ti.xcom_pull(task_ids='get_latest_candidate_ssh') }}}}" | base64 --decode)

echo "Evaluating model: $MODEL_PATH"
python3 "{EVAL_SCRIPT}" --candidate "$MODEL_PATH" --test-data "{TEST_DATA}" --auto-deploy
"""

# --- TASKS ---

# Task 0: Kiểm tra kết nối trước khi chạy
check_connectivity_task = PythonOperator(
    task_id='check_trainer_connectivity',
    python_callable=check_trainer_connectivity,
    dag=dag,
)

# Task 1: Check Data (SSH)
check_data_ssh = SSHOperator(
    task_id='check_data_availability',
    ssh_hook=ssh_hook,
    command=cmd_check_data,
    dag=dag,
)

# Task 2: Train Model (SSH)
train_model_ssh = SSHOperator(
    task_id='train_new_model',
    ssh_hook=ssh_hook,
    command=cmd_train_model,
    dag=dag,
)

# Task 3: Get Latest Candidate (SSH + XCom Push)
get_candidate_ssh = SSHOperator(
    task_id='get_latest_candidate_ssh',
    ssh_hook=ssh_hook,
    command=cmd_find_candidate,
    do_xcom_push=True,  # Quan trọng: Đẩy output (đường dẫn file) vào XCom
    dag=dag,
)

# Task 4: Evaluate & Deploy (SSH + XCom Pull từ task trước)
evaluate_deploy_ssh = SSHOperator(
    task_id='evaluate_and_deploy',
    ssh_hook=ssh_hook,
    command=cmd_evaluate,
    dag=dag,
)

# Task 5: Notification (Local Python - Chạy tại Airflow Worker)
def send_notification_logic(**context):
    # Lấy thông tin model path từ XCom của task SSH
    candidate_path = context['ti'].xcom_pull(task_ids='get_latest_candidate_ssh')
    
    print("="*60)
    print("RETRAINING PIPELINE COMPLETED")
    print("="*60)
    print(f"Candidate Model deployed from: {candidate_path}")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification_logic,
    provide_context=True,
    dag=dag,
)

# --- DEPENDENCIES ---
check_connectivity_task >> check_data_ssh >> train_model_ssh >> get_candidate_ssh >> evaluate_deploy_ssh >> notify_task