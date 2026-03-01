# Sử dụng base image giống trong docker-compose
FROM apache/airflow:2.9.3

# Chuyển sang user root để cài gói hệ thống nếu cần (tùy chọn)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Chuyển lại về user airflow để cài thư viện python
USER airflow

# Cài đặt các thư viện ML nặng ở đây
# --no-cache-dir giúp giảm dung lượng image
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    tensorflow \
    keras \
    pyspark \
    opencv-python-headless\
    kafka-python \