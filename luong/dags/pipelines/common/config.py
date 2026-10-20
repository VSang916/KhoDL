"""Cấu hình hạ tầng dùng chung. Trong docker network gọi bằng tên service (minio, clickhouse), không dùng localhost."""
import os

TZ = "Asia/Ho_Chi_Minh"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"

CH_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "mypassword")
