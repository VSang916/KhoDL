from __future__ import annotations

import io
import logging

import pandas as pd

from pipelines.common.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

log = logging.getLogger(__name__)


def client():
    from minio import Minio

    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def ensure_buckets(*buckets: str) -> None:
    c = client()
    for bucket in buckets:
        if not c.bucket_exists(bucket):
            c.make_bucket(bucket)
            log.info("Đã tạo bucket %s", bucket)


def put_bytes(c, bucket: str, key: str, data: bytes, content_type: str) -> None:
    c.put_object(bucket, key, io.BytesIO(data), length=len(data), content_type=content_type)
    log.info("Đã ghi s3://%s/%s (%d bytes)", bucket, key, len(data))


def get_bytes(c, bucket: str, key: str) -> bytes:
    resp = c.get_object(bucket, key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def list_keys(c, bucket: str, prefix: str) -> list[str]:
    return sorted(o.object_name for o in c.list_objects(bucket, prefix=prefix, recursive=True))


def get_csv(c, bucket: str, key: str, dtype: dict | None = None) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(get_bytes(c, bucket, key)), dtype=dtype, keep_default_na=False, na_values=[""])


def put_csv(c, bucket: str, key: str, df: pd.DataFrame) -> int:
    """Ghi DataFrame thành CSV (có header), trả về số dòng."""
    put_bytes(c, bucket, key, df.to_csv(index=False).encode("utf-8"), "text/csv")
    return len(df)
