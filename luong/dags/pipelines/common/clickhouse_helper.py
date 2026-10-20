"""Gọi ClickHouse qua HTTP và nạp CSV từ MinIO bằng hàm s3() (ClickHouse tự đọc, Airflow không phải tải dữ liệu)."""
from __future__ import annotations

import logging

import requests

from pipelines.common.config import (
    CH_PASSWORD, CH_URL, CH_USER, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, PROCESSED_BUCKET,
)

log = logging.getLogger(__name__)


def query(sql: str, timeout: int = 300) -> str:
    resp = requests.post(CH_URL, data=sql.encode("utf-8"), auth=(CH_USER, CH_PASSWORD), timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse lỗi {resp.status_code}: {resp.text[:500]}")
    return resp.text.strip()


def columns(tables: dict, table: str) -> list[str]:
    """tables: {tên: ("col1 Type, col2 Type", "ENGINE ...")} -> ["col1", "col2"]."""
    return [c.strip().split()[0] for c in tables[table][0].split(", ")]


def ensure_schema(db: str, tables: dict, views: dict | None = None, rebuildable: tuple = ()) -> None:
    """rebuildable: các bảng được phép xoá + tạo lại khi schema đổi (chỉ những bảng nạp lại được toàn bộ từ nguồn).
    Bảng khác mà lệch schema thì báo lỗi, không tự xoá (vd lịch sử giá Winmart chỉ có trong kho)."""
    query(f"CREATE DATABASE IF NOT EXISTS {db}")
    for table, (cols, engine) in tables.items():
        existing = query(f"SELECT name FROM system.columns WHERE database = '{db}' AND table = '{table}' ORDER BY position")
        if existing and existing.split("\n") != columns(tables, table):
            if table not in rebuildable:
                raise RuntimeError(f"Schema {db}.{table} khác định nghĩa trong code - cần migrate thủ công")
            log.warning("Schema %s.%s đã đổi -> tạo lại bảng (sẽ nạp lại từ nguồn)", db, table)
            query(f"DROP TABLE {db}.{table}")
        query(f"CREATE TABLE IF NOT EXISTS {db}.{table} ({cols}) {engine}")
    # Không dùng CREATE OR REPLACE VIEW: nó cần renameat2(), ổ đĩa mount từ Windows không hỗ trợ.
    # View không chứa dữ liệu nên xoá rồi tạo lại là an toàn.
    for name, ddl in (views or {}).items():
        query(f"DROP VIEW IF EXISTS {db}.{name}")
        query(ddl)


def load_csv_from_s3(db: str, table: str, cols: str, key: str, bucket: str = PROCESSED_BUCKET) -> None:
    url = f"http://{MINIO_ENDPOINT}/{bucket}/{key}"
    # cols có thể chứa dấu nháy đơn (vd DateTime('Asia/Ho_Chi_Minh')) nên phải thoát trước khi bọc trong '...'
    structure = cols.replace("\\", "\\\\").replace("'", "\\'")
    query(
        f"INSERT INTO {db}.{table} SELECT * FROM "
        f"s3('{url}', '{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}', 'CSVWithNames', '{structure}')"
    )
    log.info("Đã nạp s3://%s/%s vào %s.%s", bucket, key, db, table)


def count(db: str, table: str, where: str = "") -> int:
    return int(query(f"SELECT count() FROM {db}.{table}" + (f" WHERE {where}" if where else "")))
