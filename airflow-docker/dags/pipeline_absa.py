import logging
import socket
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook  
from airflow.utils.dates import days_ago

# --- CONFIGURATION ---
SPARK_MASTER_HOST = 'spark-master'
SPARK_USER = 'spark'
SPARK_PASSWORD = 'spark123'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'pipeline-absa',
    default_args=default_args,
    description='Pipeline test with SSH Operator',
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
    tags=['ssh', 'spark'],
)

# --- TẠO HOOK KẾT NỐI SSH ---
ssh_hook = SSHHook(
    remote_host=SPARK_MASTER_HOST,
    username=SPARK_USER,
    password=SPARK_PASSWORD,
    port=22,
    keepalive_interval=10  
)

# --- HEALTH CHECKS ---
def check_kafka_health():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('kafka', 29092)) 
        sock.close()
        if result == 0:
            logging.info("SUCCESS: Kafka is accessible")
            return True
        raise Exception("Cannot connect to Kafka")
    except Exception as e:
        logging.error(str(e))
        raise

def check_postgres_health():
    import psycopg2 
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='airflow',
            user='airflow',
            password='airflow'
        )
        conn.close()
        logging.info("SUCCESS: PostgreSQL is accessible")
    except Exception as e:
        logging.error(str(e))
        raise

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

# --- COMMANDS ---

# Producer: Chạy bằng python3 thuần là OK vì dùng thư viện kafka-python (pip)
cmd_producer = """
echo "Waiting 15s for Consumer to warm up..."
sleep 15s
cd /app
export PYTHONPATH=$PYTHONPATH:/app
python3 src2/producer.py 
"""

# Consumer: Cần dùng spark-submit để nạp Driver Kafka/Postgres
JAVA_PATH = "/opt/java/openjdk" 
SPARK_PATH = "/opt/spark"

cmd_consumer = f"""
export JAVA_HOME={JAVA_PATH}
export SPARK_HOME={SPARK_PATH}
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0 \
    --conf spark.executor.memory=2g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.sql.execution.arrow.maxRecordsPerBatch=20 \
    --conf spark.task.maxFailures=1 \
    /app/src2/consumer.py
"""

# --- SSH TASKS ---

run_consumer_ssh = SSHOperator(
    task_id='run_consumer_ssh',
    ssh_hook=ssh_hook,
    command=cmd_consumer,
    cmd_timeout=None, 
    dag=dag,
)

run_producer_ssh = SSHOperator(
    task_id='run_producer_ssh',
    ssh_hook=ssh_hook,
    command=cmd_producer,
    cmd_timeout=None,
    dag=dag,
)

# --- DEPENDENCIES ---
task_check_kafka >> task_check_postgres
task_check_postgres >> run_consumer_ssh
task_check_postgres >> run_producer_ssh