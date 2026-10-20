"""CDC bằng so sánh snapshot (nguồn không có log thay đổi / cột updated_at nên không dùng được Debezium).

Mỗi lần chạy:
  1. Dựng snapshot trạng thái hiện tại của nguồn (1 dòng / khoá) + row_hash trên các cột cần theo dõi.
  2. So với snapshot ĐÃ COMMIT gần nhất có dt < hôm nay  -> danh sách thay đổi I (mới) / U (đổi) / D (mất).
  3. Ghi snapshot mới vào MinIO, nạp thay đổi vào ClickHouse.
  4. Chỉ khi nạp thành công mới ghi _meta.json (= commit).

Lưu trên MinIO:
  processed-data/<source>/_cdc/<scope>/dt=YYYY-MM-DD/snapshot*.csv  (1 nguồn có thể có nhiều snapshot)
  processed-data/<source>/_cdc/<scope>/dt=YYYY-MM-DD/_meta.json   <- có file này mới tính là đã commit

Vì so với snapshot của NGÀY TRƯỚC (không phải lần chạy trước), chạy lại cùng ngày cho ra cùng kết quả (idempotent).
Lần chạy lỗi giữa chừng không commit, nên lần sau tự so với mốc cũ hơn và không bỏ sót thay đổi.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import re

import pandas as pd

from pipelines.common import minio_helper as mh
from pipelines.common.config import PROCESSED_BUCKET

log = logging.getLogger(__name__)

OP_INSERT, OP_UPDATE, OP_DELETE = "I", "U", "D"


# ================================================================ hash + diff (hàm thuần)
def _fmt(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return f"{float(v):.4f}"  # 15000, 15000.0 và "15000.0" đọc lại từ CSV cho cùng 1 hash
    return str(v)


def row_hash(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    joined = df[cols].apply(lambda r: "|".join(_fmt(v) for v in r), axis=1)
    return joined.map(lambda s: hashlib.md5(s.encode("utf-8")).hexdigest())


def diff_snapshots(old: pd.DataFrame | None, new: pd.DataFrame, keys: list[str], cols: list[str]) -> pd.DataFrame:
    """old/new phải có cột row_hash. Trả về keys + op + <col>_old + <col>_new cho mọi khoá có thay đổi."""
    out_cols = keys + ["op"] + [f"{c}_old" for c in cols] + [f"{c}_new" for c in cols]
    if old is None or old.empty:
        res = new[keys + cols].rename(columns={c: f"{c}_new" for c in cols})
        res["op"] = OP_INSERT
        for c in cols:
            res[f"{c}_old"] = None
        return res[out_cols].reset_index(drop=True)

    m = old[keys + cols + ["row_hash"]].merge(
        new[keys + cols + ["row_hash"]], on=keys, how="outer", suffixes=("_old", "_new"), indicator=True
    )
    m["op"] = None
    m.loc[m["_merge"] == "right_only", "op"] = OP_INSERT
    m.loc[m["_merge"] == "left_only", "op"] = OP_DELETE
    m.loc[(m["_merge"] == "both") & (m["row_hash_old"] != m["row_hash_new"]), "op"] = OP_UPDATE
    return m[m["op"].notna()][out_cols].reset_index(drop=True)


# ================================================================ lưu trạng thái trên MinIO
def _prefix(source: str, scope: str) -> str:
    return f"{source}/_cdc/{scope}"


def scope_name(parts: list[str]) -> str:
    """Phạm vi dữ liệu của lần chạy. Chỉ so snapshot cùng phạm vi (vd tải 'Vải' không so với tải 'tất cả')."""
    if not parts:
        return "all"
    return "__".join(re.sub(r"[^\w-]+", "_", p, flags=re.UNICODE) for p in sorted(parts))


def committed_dts(client, source: str, scope: str) -> list[str]:
    prefix = _prefix(source, scope) + "/"
    dts = []
    for obj in client.list_objects(PROCESSED_BUCKET, prefix=prefix, recursive=True):
        m = re.search(r"/dt=(\d{4}-\d{2}-\d{2})/_meta\.json$", obj.object_name)
        if m:
            dts.append(m.group(1))
    return sorted(dts)


def base_dt(client, source: str, scope: str, today: str) -> str | None:
    """Mốc so sánh: snapshot đã commit gần nhất TRƯỚC ngày hôm nay."""
    older = [d for d in committed_dts(client, source, scope) if d < today]
    return older[-1] if older else None


def read_meta(client, source: str, scope: str, dt: str) -> dict:
    return json.loads(mh.get_bytes(client, PROCESSED_BUCKET, f"{_prefix(source, scope)}/dt={dt}/_meta.json"))


def load_snapshot(client, source: str, scope: str, dt: str | None, dtype: dict | None = None,
                  name: str = "snapshot") -> pd.DataFrame | None:
    if dt is None:
        return None
    data = mh.get_bytes(client, PROCESSED_BUCKET, f"{_prefix(source, scope)}/dt={dt}/{name}.csv")
    return pd.read_csv(io.BytesIO(data), dtype=dtype, keep_default_na=False, na_values=[""])


def save_snapshot(client, source: str, scope: str, dt: str, df: pd.DataFrame, name: str = "snapshot") -> str:
    key = f"{_prefix(source, scope)}/dt={dt}/{name}.csv"
    mh.put_csv(client, PROCESSED_BUCKET, key, df)
    return key


def commit(client, source: str, scope: str, dt: str, meta: dict) -> None:
    key = f"{_prefix(source, scope)}/dt={dt}/_meta.json"
    mh.put_bytes(client, PROCESSED_BUCKET, key, json.dumps(meta, ensure_ascii=False, default=str).encode("utf-8"), "application/json")
    log.info("CDC commit %s/%s dt=%s", source, scope, dt)
