<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology" style="border: none;">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)">
  </a>
</p>

<h1 align="center"><b>CS331 - Advanced Computer Vision</b></h1>

# **Real-Time Vehicle Counting System**

A distributed streaming system that counts vehicles in traffic camera footage in real time.
Video frames are published to Kafka, consumed by a Spark Structured Streaming job that runs YOLO11 object detection on each frame, and the per-frame vehicle counts are persisted to PostgreSQL and visualised on a live Streamlit dashboard.
Apache Airflow orchestrates and health-checks the pipeline.

- **Frame-level streaming ingestion** - any video file or webcam becomes a Kafka producer, with JPEG compression, resizing and FPS rate limiting.
- **Distributed inference** - YOLO11n runs inside a Spark `pandas_udf`, so detection scales horizontally across Spark workers with the model loaded once per worker.
- **Dependency-light inference runtime** - the ONNX model is executed through OpenCV DNN, so Spark workers need neither PyTorch nor Ultralytics.
- **Multi-camera support** - every message carries a `cam_id`, so counts from several cameras are tracked, compared and aggregated independently.
- **Live dashboard** - a 2-second auto-refreshing Streamlit app with KPIs, time series, per-camera breakdowns and a camera-vs-hour traffic heatmap.
- **Containerised stack** - a single Docker Compose file brings up Kafka, Zookeeper, Kafdrop, a Spark master/worker cluster, Airflow on Celery, and a TensorFlow trainer container.

<p align="center">
  <img src="thumbnail.png" width="600" alt="thumbnail">
</p>

---

## 👥 Group information 

| STT | Student ID | Full Name | Role | Github | Email |
| --- | --- | --- | --- | --- | --- |
| 1 | 23521329 | Nguyễn Văn Quyền | Developer | [quyen244](https://github.com/quyen244) | 23521329@gm.uit.edu.vn |

---

## Table of Contents

- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data](#data)
- [Methodology](#methodology)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Monitoring & Service Endpoints](#monitoring--service-endpoints)
- [Airflow DAGs](#airflow-dags)
- [Configuration Reference](#configuration-reference)
- [Known Limitations](#known-limitations)
- [References](#references)

---

## Project Overview

### Problem Statement

Traffic management requires continuous, quantitative knowledge of how many vehicles pass through a given point.
Running object detection on a single machine over a single video file does not reflect the real operating conditions of a city-scale deployment, where many cameras emit frames simultaneously and the results must be available to operators immediately.

### Motivation

This project treats vehicle counting as a **streaming data problem** rather than a batch computer-vision script.
Instead of processing a video end to end and printing a number, each camera becomes an independent producer of frames, inference is distributed over a cluster, and results flow into a database that a dashboard reads continuously.

### Goals

1. Ingest video from heterogeneous sources (recorded files, webcams, multiple simultaneous cameras) into a single message bus.
2. Perform vehicle detection and counting on each frame with a modern single-stage detector.
3. Scale inference horizontally rather than vertically, keeping per-worker model loading cost amortised.
4. Persist counts in a queryable store and expose them through a real-time dashboard.
5. Orchestrate and health-check the whole pipeline with a workflow engine.

---

## Key Features

### 1. Video-to-Kafka Producer

`src/producer/producer.py` implements the `VideoKafkaProducer` class:

- Accepts either a **video file path** or a **camera index** (`0`, `1`, ...) as input; a numeric argument automatically opens the corresponding webcam.
- Resizes every frame to **640x640**, matching the YOLO input resolution and cutting network bandwidth.
- Encodes frames as **JPEG at quality 90**, then **Base64**, so they can travel inside a JSON payload.
- Sends asynchronously with success/error callbacks, 5 retries and a 20-second request timeout.
- Applies **rate limiting**: the target FPS is enforced by subtracting the actual processing time from the sleep interval, so the stream does not saturate the broker.
- Releases the capture device and flushes the producer cleanly on `Ctrl+C`.

Message payload:

```json
{
  "cam_id": "CAM_HAN_01",
  "timestamp": "2026-02-09 03:28:55.123456",
  "frame_id": 128,
  "frame_data": "<base64-encoded JPEG>"
}
```

### 2. Spark Streaming Vehicle Counter

`src/processing/consumer.py` implements the `VehicleCountingPipeline` class:

- Subscribes to the Kafka topic from `startingOffsets=earliest` with `failOnDataLoss=false`, so the job survives broker log retention.
- Throttles itself with `maxOffsetsPerTrigger=20` and `spark.sql.execution.arrow.maxRecordsPerBatch=20`, keeping each micro-batch small enough that heavy per-frame inference does not exhaust executor memory.
- Parses the JSON payload against an explicit `StructType` schema.
- Runs detection in a **scalar iterator `pandas_udf`**, which receives an *iterator* of pandas Series. The model is cached on the function object (`count_vehicles_udf._net`), so each Spark worker process loads the ONNX graph exactly once instead of once per batch.
- Writes results with `foreachBatch` + JDBC in `append` mode, skipping empty batches.
- Wipes its checkpoint directory on startup for clean reruns during development.

### 3. Real-Time Dashboard

`src/dashboard/main.py` is a wide-layout Streamlit app that re-queries the database every **2 seconds** via `streamlit_autorefresh` (rather than a blocking loop) and renders, over the most recent 500 rows:

| Panel | Visualisation |
| --- | --- |
| Total Vehicles | KPI metric - sum of `vehicle_count` |
| Active Cameras | KPI metric - distinct `cam_id` count |
| Avg Vehicles / Camera | KPI metric - mean `vehicle_count` |
| Vehicle Count Over Time | Plotly line chart, one line per camera |
| Total Vehicles per Camera | Plotly bar chart |
| Traffic Distribution by Camera | Plotly pie chart |
| Traffic Heatmap | Plotly density heatmap, camera vs hour of day |

### 4. Orchestration

The `vehicle_counting_pipeline` Airflow DAG validates that Kafka and PostgreSQL are reachable before the pipeline runs.
`pipeline-absa` demonstrates the `SSHOperator` pattern used to launch `spark-submit` on the Spark master container with the required Kafka and PostgreSQL JARs.

---

## Architecture

```
┌──────────────┐
│  Video file  │
│   / Webcam   │
└──────┬───────┘
       │  OpenCV read -> resize 640x640 -> JPEG(q90) -> Base64
       v
┌──────────────────────┐        ┌───────────┐
│   Kafka Producer     │──────> │   Kafka   │  topic: vehicle_stream
│   (kafka-python)     │        │ +Zookeeper│  (Kafdrop UI on :9000)
└──────────────────────┘        └─────┬─────┘
                                      │ maxOffsetsPerTrigger = 20
                                      v
                      ┌────────────────────────────────┐
                      │  Spark Structured Streaming    │
                      │  ┌──────────────────────────┐  │
                      │  │ pandas_udf               │  │
                      │  │  Base64 -> JPEG decode   │  │
                      │  │  blobFromImage (640)     │  │
                      │  │  YOLO11n ONNX (cv2.dnn)  │  │
                      │  │  conf 0.5 -> class filter│  │
                      │  │  NMS 0.4 -> count        │  │
                      │  └──────────────────────────┘  │
                      └───────────────┬────────────────┘
                                      │ foreachBatch -> JDBC append
                                      v
                            ┌───────────────────┐
                            │    PostgreSQL     │
                            │    traffic_db     │
                            │  vehicle_counts   │
                            └─────────┬─────────┘
                                      │ psycopg2, 2s poll
                                      v
                            ┌───────────────────┐
                            │ Streamlit + Plotly│
                            │     Dashboard     │
                            └───────────────────┘

         Apache Airflow (CeleryExecutor) - health checks & orchestration
```

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Language | Python 3 |
| Message broker | Apache Kafka 7.4.0 (Confluent) + Zookeeper |
| Kafka UI | Kafdrop |
| Stream processing | Apache Spark 3.5.0 (Structured Streaming, PySpark) |
| Object detection | YOLO11n, exported to ONNX |
| Inference runtime | OpenCV DNN (`cv2.dnn.readNetFromONNX`) |
| Video I/O | OpenCV (`opencv-python-headless`) |
| Database | PostgreSQL |
| Dashboard | Streamlit + Plotly Express |
| Orchestration | Apache Airflow 2.9.3 (CeleryExecutor, Redis broker) |
| Training container | TensorFlow 2.15.0 image with SSH access |
| Containerisation | Docker + Docker Compose |

### Spark Packages

The streaming job pulls two JARs at runtime:

- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
- `org.postgresql:postgresql:42.5.0`

---

## Data

### Sample Videos (`data/`)

| File | Size | Purpose |
| --- | --- | --- |
| `traffic_1.mp4` | ~122 KB | Short traffic clip, camera 1 |
| `traffic_2.mp4` | ~126 KB | Short traffic clip, camera 2 |
| `traffic_3.mp4` | ~126 KB | Short traffic clip, camera 3 |
| `d756ba9c_034_clip_001_0000_0010_Y.mp4` | ~28 MB | Longer traffic clip |

> `data/` and `models/` are excluded from version control by `.gitignore`. Provide your own videos and model weights after cloning.

### Models (`models/`)

| File | Description |
| --- | --- |
| `yolo11n.pt` | YOLO11-nano PyTorch weights (Ultralytics) |
| `yolo11n.onnx` | ONNX export, the artifact actually used by the Spark consumer |

`models/__init__.py` contains a short helper that loads `yolo11n.pt` through Ultralytics, which is how the `.pt` file is fetched before being exported.

### Detected Classes

Detection uses the standard COCO class indices and keeps only vehicle classes:

| COCO ID | Class |
| --- | --- |
| 2 | Car |
| 3 | Motorcycle |
| 5 | Bus |
| 7 | Truck |

---

## Methodology

### Per-Frame Detection Pipeline

1. **Decode** - Base64 string to byte buffer to `cv2.imdecode` BGR image. Empty or malformed payloads short-circuit to a count of `0`.
2. **Preprocess** - `cv2.dnn.blobFromImage` with scale `1/255`, target size `640x640`, `swapRB=True`, no cropping.
3. **Inference** - `net.forward()` through the OpenCV DNN backend, pinned to `DNN_BACKEND_OPENCV` / `DNN_TARGET_CPU`.
4. **Decode output** - the YOLOv8/v11 head emits shape `(1, 84, 8400)`; it is transposed to `(8400, 84)` so that each row is one candidate box of `[cx, cy, w, h, 80 class scores]`.
5. **Filter** - per row, `cv2.minMaxLoc` finds the top class score. Rows are kept only if `score >= 0.5` **and** the class is one of the four vehicle classes. Centre-form boxes are converted to corner form and rescaled to the original image dimensions.
6. **Non-Maximum Suppression** - `cv2.dnn.NMSBoxes` with confidence 0.5 and IoU 0.4 removes duplicate detections.
7. **Count** - the number of surviving indices becomes `vehicle_count` for that frame.

### Design Decisions

| Decision | Rationale |
| --- | --- |
| ONNX + OpenCV DNN instead of Ultralytics on the workers | Avoids installing PyTorch and Ultralytics in every Spark executor; OpenCV is already required for frame decoding. |
| Model cached as a function attribute | A `pandas_udf` is invoked per batch; loading the graph each time would dominate runtime. Caching on the function object persists it for the lifetime of the Python worker. |
| Scalar **iterator** UDF | Gives an explicit place to run once-per-worker setup before iterating over batches, which the plain scalar UDF form does not. |
| `maxOffsetsPerTrigger=20` and `maxRecordsPerBatch=20` | Caps how many frames an executor holds in memory at once; frames are large relative to typical streaming records. |
| Frames resized and JPEG-compressed in the producer | Shifts the cost to the edge, so the broker carries tens of KB per frame instead of raw bitmaps. |
| Base64 inside JSON | Keeps the payload self-describing and lets Spark parse it with `from_json` and a plain `StringType` column. |
| Rate limiting at the producer | Prevents a fast local file read from flooding Kafka faster than inference can drain it. |

---

## Project Structure

```
video-counting/
├── src/
│   ├── producer/
│   │   ├── producer.py          # VideoKafkaProducer: video/webcam -> Kafka
│   │   ├── config.py            # (empty placeholder)
│   │   └── requirements.txt     # kafka-python, opencv-python-headless, numpy
│   ├── processing/
│   │   └── consumer.py          # Spark Streaming + YOLO11 ONNX UDF -> PostgreSQL
│   └── dashboard/
│       └── main.py              # Streamlit real-time dashboard
├── models/
│   ├── __init__.py              # Ultralytics YOLO loader helper
│   ├── yolo11n.pt               # PyTorch weights
│   ├── yolo11n.onnx             # ONNX export used in inference
│   └── candidates/              # Candidate models produced by the retraining DAG
├── data/
│   └── *.mp4                    # Sample traffic videos
├── sql/
│   └── init_db.sql              # Legacy schema (see Known Limitations)
├── scripts/
│   └── running_src1.sh          # One-shot local launcher for the whole pipeline
├── airflow-docker/
│   ├── docker-compose.yaml      # Kafka, Zookeeper, Kafdrop, Spark, Airflow, trainer
│   ├── Dockerfile               # Airflow image + ML libraries
│   ├── Dockerfile.spark         # Spark 3.5.0 + OpenCV/ONNX Runtime + SSH
│   ├── Dockerfile.trainer       # TensorFlow 2.15.0 + SSH
│   ├── entrypoint_master.sh     # Starts sshd, then Spark master or worker
│   ├── entrypoint_trainer.sh    # Starts sshd in the trainer container
│   ├── .env                     # AIRFLOW_UID
│   └── dags/
│       ├── pipeline.py          # vehicle_counting_pipeline (main DAG)
│       ├── pipeline_absa.py     # pipeline-absa (SSH / spark-submit reference)
│       ├── absa_pipeline_dag.py # absa_model_retraining (legacy)
│       └── simple_dag.py        # airflow_tutorial_dag (smoke test)
├── MODEL_LOADING_FIX.md         # Legacy note on Keras model loading
├── requirements.txt             # (empty)
└── README.md
```

---

## Installation & Setup

### Prerequisites

- Docker Desktop (Compose v2) - required for `host.docker.internal` resolution
- Python 3.8+
- PostgreSQL 13+ running on the **host machine**
- At least ~10 GB RAM available to Docker (the Airflow Celery worker alone is capped at 6 GB)

### 1. Clone and prepare the model

```bash
git clone https://github.com/quyen244/vehicle-counting-system.git
cd vehicle-counting-system
```

Download the YOLO11n weights and export them to ONNX:

```bash
pip install ultralytics
python -c "from ultralytics import YOLO; YOLO('yolo11n.pt').export(format='onnx')"
```

Place `yolo11n.onnx` in `models/` and the sample videos in `data/`.

### 2. Create the PostgreSQL database and table

The consumer writes `cam_id`, `stream_timestamp` and `vehicle_count`; the dashboard additionally reads `created_at`, which therefore needs a server-side default.
`sql/init_db.sql` still contains the schema of an earlier project, so create the table explicitly:

```sql
CREATE DATABASE traffic_db;
\c traffic_db

CREATE TABLE IF NOT EXISTS vehicle_counts (
    id               SERIAL PRIMARY KEY,
    cam_id           VARCHAR(50)  NOT NULL,
    stream_timestamp VARCHAR(50),
    vehicle_count    INTEGER      NOT NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vehicle_counts_created_at
    ON vehicle_counts (created_at DESC);
```

Make sure PostgreSQL accepts connections from Docker containers (`listen_addresses = '*'` in `postgresql.conf` plus a matching `pg_hba.conf` entry), because the Spark job connects through `host.docker.internal`.

### 3. Adjust the host path in Docker Compose

`airflow-docker/docker-compose.yaml` mounts the project with an absolute Windows path.
Replace every occurrence of `D:/Projects/Assignment/video-counting` with your own checkout path:

| Service | Mount point |
| --- | --- |
| `airflow-*` | `/opt/airflow/project` |
| `spark-master`, `spark-worker` | `/app` |
| `model-trainer` | `/opt/project` |

### 4. Start the infrastructure

```bash
cd airflow-docker
docker compose up -d --build
```

This builds `spark-custom` from `Dockerfile.spark` and the trainer image from `Dockerfile.trainer`, then starts Zookeeper, Kafka, Kafdrop, the Spark master/worker pair, PostgreSQL + Redis for Airflow metadata, and the Airflow webserver, scheduler, Celery worker and triggerer.

### 5. Install local Python dependencies

For the producer and dashboard, which run on the host:

```bash
python -m venv venv
source venv/Scripts/activate      # Windows (Git Bash); use venv/bin/activate on Linux/macOS
pip install -r src/producer/requirements.txt
pip install streamlit streamlit-autorefresh plotly psycopg2-binary pandas pyspark==3.5.0
```

> The root `requirements.txt` is currently empty; `src/producer/requirements.txt` holds the producer dependencies.

---

## Usage

### Option A: One-shot launcher

`scripts/running_src1.sh` starts Docker Compose, activates the virtualenv, and launches the consumer, dashboard and producer in the right order, with a `Ctrl+C` trap that cleans up the background processes.

```bash
bash scripts/running_src1.sh
```

> The script resolves paths relative to the directory *above* the repository and assumes a `venv/Scripts/activate` layout, so review the paths before using it on a different machine.

### Option B: Run each component manually

**1. Start the Spark consumer** (inside the Spark master container, so that the `kafka:29092` and `/app/models` paths resolve):

```bash
docker exec -it spark-master bash

export JAVA_HOME=/opt/java/openjdk
export SPARK_HOME=/opt/spark
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.sql.execution.arrow.maxRecordsPerBatch=20 \
  --conf spark.task.maxFailures=1 \
  /app/src/processing/consumer.py
```

Give the consumer around 15 seconds to register with Kafka before starting a producer.

**2. Start one or more producers** (from the host):

```bash
cd src/producer

# Stream a recorded video as camera 1
python producer.py --input ../../data/traffic_1.mp4 --cam_id CAM_HAN_01

# Stream a second video as camera 2, at a lower rate
python producer.py --input ../../data/traffic_2.mp4 --cam_id CAM_HAN_02 --fps 5

# Stream a live webcam
python producer.py --input 0 --cam_id CAM_LIVE_01
```

**Producer CLI arguments:**

| Argument | Required | Default | Description |
| --- | --- | --- | --- |
| `--input` | yes | - | Video file path, or camera index (`0`, `1`, ...) |
| `--cam_id` | yes | - | Unique camera identifier, e.g. `CAM_01` |
| `--topic` | no | `vehicle_stream` | Kafka topic name |
| `--server` | no | `localhost:9092` | Kafka bootstrap servers |
| `--fps` | no | `10` | Target frames per second to publish |

Use `localhost:9092` when running the producer on the host and `kafka:29092` when running it inside a container on `data-network`.

**3. Start the dashboard** (from the host):

```bash
streamlit run src/dashboard/main.py
```

Open http://localhost:8501.

### Verifying the pipeline

```bash
# Inspect the topic and its messages in Kafdrop at http://localhost:9000

# Confirm rows are landing
psql -U postgres -d traffic_db -c "SELECT COUNT(*) FROM vehicle_counts;"
psql -U postgres -d traffic_db -c "SELECT * FROM vehicle_counts ORDER BY created_at DESC LIMIT 10;"

# Per-camera totals
psql -U postgres -d traffic_db -c "SELECT cam_id, SUM(vehicle_count), AVG(vehicle_count) FROM vehicle_counts GROUP BY cam_id;"
```

---

## Monitoring & Service Endpoints

| Service | URL | Credentials |
| --- | --- | --- |
| Airflow Web UI | http://localhost:8080 | `airflow` / `airflow` |
| Spark Master UI | http://localhost:8081 | - |
| Kafdrop (Kafka UI) | http://localhost:9000 | - |
| Streamlit Dashboard | http://localhost:8501 | - |
| Celery Flower | http://localhost:5555 | enable with `--profile flower` |
| Spark Master SSH | `localhost:22` | `spark` / `spark123` |
| Model Trainer SSH | `localhost:2222` | `root` / `root123` |

Spark exposes `7077` for cluster submissions, and Kafka exposes `9092` (host listener) and `29092` (in-network listener).

Optional Compose profiles:

```bash
docker compose --profile flower up -d                  # Celery monitoring
docker compose --profile debug run --rm airflow-cli    # Airflow CLI shell
```

---

## Airflow DAGs

### `vehicle_counting_pipeline` - main DAG

Defined in `airflow-docker/dags/pipeline.py`.
Schedule: `None` (manual trigger). Tags: `kafka`, `spark`, `yolo`, `vehicle-counting`.

```
check_kafka_health    ─┐
                       ├──> run_pipeline
check_postgres_health ─┘
```

| Task | Behaviour |
| --- | --- |
| `check_kafka_health` | Opens a TCP socket to `host.docker.internal:9092` with a 5-second timeout and fails the task if the connection is refused. |
| `check_postgres_health` | Opens a `psycopg2` connection to `traffic_db` on `host.docker.internal:5432` and fails the task on error. |
| `run_pipeline` | Placeholder task. The producer/consumer launch functions (`start_kafka_producer`, `start_spark_consumer`, `monitor_pipeline`) are present in the file but commented out. |

The DAG resolves scripts relative to `PROJECT_ROOT = /opt/airflow/project`, which is where Compose mounts the repository into the Airflow containers.

### `pipeline-absa` - SSH / spark-submit reference

Defined in `airflow-docker/dags/pipeline_absa.py`.
Demonstrates the pattern for driving the pipeline remotely: health checks, then an `SSHOperator` into `spark-master` (`spark` / `spark123`) that runs `spark-submit` with the Kafka and PostgreSQL packages, followed by a producer task that waits 15 seconds for the consumer to warm up.
The script paths it references belong to an earlier project layout.

### `absa_model_retraining` - legacy

Defined in `airflow-docker/dags/absa_pipeline_dag.py`.
A weekly (`0 2 * * 0`) retraining workflow from the project's earlier aspect-based sentiment analysis phase: `check_trainer_connectivity` -> `check_data_availability` -> `train_new_model` -> `get_latest_candidate_ssh` -> `evaluate_and_deploy` -> `send_notification`, executed over SSH against the `model-trainer` container.
The `model-trainer` service and the `models/candidates/` directory exist to support it.

### `airflow_tutorial_dag`

Defined in `airflow-docker/dags/simple_dag.py`. A three-task Bash/Python smoke test for verifying the Airflow installation.

---

## Configuration Reference

Configuration currently lives as class constants in the source files rather than in environment variables.

### `AppConfig` in `src/processing/consumer.py`

| Setting | Value |
| --- | --- |
| `KAFKA_BOOTSTRAP` | `kafka:29092` |
| `KAFKA_TOPIC` | `vehicle_stream` |
| `PG_URL` | `jdbc:postgresql://host.docker.internal:5432/traffic_db` |
| `PG_TABLE` | `vehicle_counts` |
| `PG_USER` / `PG_PASS` | `postgres` / `1234` |
| `MODEL_PATH` | `/app/models/yolo11n.onnx` |
| `CHECKPOINT_DIR` | `/tmp/checkpoints/vehicle-counting` |

Detection hyperparameters inside `count_vehicles_udf`: `INPUT_SIZE=640`, `CONF_THRESHOLD=0.5`, `NMS_THRESHOLD=0.4`, `TARGET_CLASSES=[2, 3, 5, 7]`.

### `DBConfig` in `src/dashboard/main.py`

| Setting | Value |
| --- | --- |
| `DB_NAME` | `traffic_db` |
| `HOST` / `PORT` | `localhost` / `5432` |
| `USER` / `PASSWORD` | `postgres` / `1234` |
| Query limit | 500 most recent rows |
| Refresh interval | 2000 ms |

### Spark and Airflow (`docker-compose.yaml`)

| Setting | Value |
| --- | --- |
| Spark worker cores | 2 |
| Spark worker memory | 2 GB |
| Airflow Celery worker memory limit | 6 GB |
| Airflow executor | `CeleryExecutor` (Redis broker, PostgreSQL backend) |

---

## Known Limitations

- **Counting, not tracking.** `vehicle_count` is the number of vehicles *visible in a single frame*, not the number of unique vehicles that have crossed a line. Unique-vehicle totals would require an object tracker (for example ByteTrack or SORT) plus line-crossing logic; the dashboard's "Total Vehicles" KPI therefore sums per-frame observations.
- **Stale SQL schema.** `sql/init_db.sql` still defines `sentiment_analysis` and `model_versions` from the earlier ABSA project and does not create `vehicle_counts`. Use the DDL in [Installation & Setup](#installation--setup) instead.
- **Hardcoded absolute paths.** `docker-compose.yaml` mounts `D:/Projects/Assignment/video-counting`, and `scripts/running_src1.sh` assumes a specific directory layout; both need editing for a different checkout location.
- **Credentials in source.** Database and SSH passwords are literals in the code and Dockerfiles, which is acceptable for a local assignment but not for deployment.
- **`host.docker.internal` dependency.** The consumer and the Airflow health checks reach the host database through `host.docker.internal`, which is a Docker Desktop feature and is not available on plain Linux Docker without `--add-host`.
- **CPU-only inference.** OpenCV DNN is pinned to `DNN_TARGET_CPU`, and throughput is deliberately capped at 20 frames per micro-batch.
- **Empty files.** `src/producer/config.py` and the root `requirements.txt` are placeholders.
- **Legacy artifacts.** `MODEL_LOADING_FIX.md`, `models/inference_model.keras`, `models/vietnamese_sentiment_model.keras`, `models/candidates/` and the two ABSA DAGs belong to the project's earlier sentiment-analysis phase.

---

## References

- [YOLO11 - Ultralytics Documentation](https://docs.ultralytics.com/models/yolo11/)
- [OpenCV DNN Module](https://docs.opencv.org/4.x/d6/d0f/group__dnn.html)
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [PySpark pandas UDFs](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Express](https://plotly.com/python/plotly-express/)
- [Kafdrop](https://github.com/obsidiandynamics/kafdrop)
