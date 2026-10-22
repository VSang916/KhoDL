"""CDC nguồn TTNS: mỗi lần xuất là TOÀN BỘ lịch sử, không có khoá, dòng trùng hợp lệ, và cộng tác viên có thể
sửa số liệu các tháng cũ. Vì vậy so ở mức tổng hợp tháng (fact_price_monthly), nơi có khoá rõ ràng:
    (product_id, market_id, date_id, loai_gia, don_vi_tinh)

Phân loại thay đổi (change_kind):
    initial   : lần đầu theo dõi (chưa có snapshot mốc)
    new_month : tháng mới hơn mọi tháng đã có  -> dữ liệu mới bình thường
    backfill  : thêm khoá vào tháng đã có (báo giá muộn, thêm tỉnh/mặt hàng)
    revision  : khoá đã có nhưng số liệu đổi  -> SỬA DỮ LIỆU QUÁ KHỨ
    removed   : khoá biến mất (nguồn xoá, hoặc đổi tên mặt hàng -> nên thêm vào NAME_ALIASES)
"""
from __future__ import annotations

import pandas as pd

from pipelines.common.cdc import OP_DELETE, OP_INSERT, OP_UPDATE, diff_snapshots, row_hash
from pipelines.ttns.schema import columns

KEYS = ["product_id", "market_id", "date_id", "loai_gia", "don_vi_tinh"]
TRACKED = ["n_obs", "n_outliers", "avg_price", "median_price", "min_price", "max_price"]
# Nguồn trả toàn bộ lịch sử nên khoá biến mất hàng loạt là bất thường (export lỗi, đổi cách đặt tên...): chặn lại
MAX_DELETE_RATIO = 0.2

FACT_COLS = columns("fact_price_monthly", with_cdc=False)
SNAPSHOT_COLS = FACT_COLS + ["row_hash"]
SNAPSHOT_DTYPE = {"loai_gia": str, "cap_gia": str, "don_vi_tinh": str}
LOG_COLS = columns("cdc_log")


def compute(prev: pd.DataFrame | None, fact: pd.DataFrame, run_date: str, scope: str, version: int):
    """Trả về (cdc_log, fact_upsert, snapshot_mới, thống kê).
    fact_upsert: dòng I/U lấy từ bản mới (is_deleted=0), dòng D lấy từ snapshot cũ (is_deleted=1)."""
    snapshot = fact[FACT_COLS].copy()
    snapshot["row_hash"] = row_hash(snapshot, TRACKED)
    if prev is not None:
        # Snapshot mốc có thể được ghi bằng schema cũ (vd trước khi thêm n_valid, sum_price): bổ sung cột thiếu.
        # row_hash chỉ tính trên TRACKED (không đổi) nên so sánh vẫn đúng.
        prev = prev.reindex(columns=SNAPSHOT_COLS)
    d = diff_snapshots(prev, snapshot, KEYS, TRACKED)

    initial = prev is None or prev.empty
    last_month = None if initial else int(prev["date_id"].max())
    if initial:
        kind = pd.Series("initial", index=d.index)
    else:
        kind = d["op"].map({OP_UPDATE: "revision", OP_DELETE: "removed"})
        is_new = d["op"] == OP_INSERT
        kind[is_new] = (d.loc[is_new, "date_id"] > last_month).map({True: "new_month", False: "backfill"})
    d["change_kind"] = kind

    log = d.rename(columns={f"{c}_{s}": f"{s}_{c}" for c in TRACKED for s in ("old", "new")})
    log["run_date"], log["scope"] = run_date, scope
    log = log[LOG_COLS].reset_index(drop=True)
    for c in ("old_n_obs", "new_n_obs"):
        log[c] = pd.to_numeric(log[c]).astype("Int64")

    changed = d[KEYS + ["op"]]
    upserts = snapshot.merge(changed[changed["op"] != OP_DELETE][KEYS], on=KEYS)[FACT_COLS].assign(is_deleted=0)
    deletes = (
        prev.merge(changed[changed["op"] == OP_DELETE][KEYS], on=KEYS)[FACT_COLS].assign(is_deleted=1)
        if not initial else pd.DataFrame(columns=FACT_COLS + ["is_deleted"])
    )
    fact_upsert = pd.concat([upserts, deletes], ignore_index=True).assign(version=version)
    fact_upsert = fact_upsert[columns("fact_price_monthly")]
    for c in ("n_obs", "n_outliers", "n_valid", "date_id", "product_id", "market_id"):
        fact_upsert[c] = pd.to_numeric(fact_upsert[c]).astype("Int64")   # CSV ghi "4" chứ không "4.0" (cột UInt32)

    stats = {
        "base_rows": 0 if initial else len(prev),
        "new_rows": len(snapshot),
        "last_month_before": last_month,
        **log["change_kind"].value_counts().to_dict(),
    }
    stats["delete_ratio"] = round(stats.get("removed", 0) / stats["base_rows"], 4) if stats["base_rows"] else 0.0
    return log, fact_upsert, snapshot[SNAPSHOT_COLS], stats


# ---------------------------------------------------------------- price_clean
# price_clean không có khoá từng dòng (dòng trùng hợp lệ) nên CDC theo NHÓM khoá: hash của toàn bộ dòng trong nhóm.
# Tách riêng khỏi fact vì fact bỏ các nhóm chỉ toàn giá bất thường, còn price_clean vẫn giữ các dòng đó.
CLEAN_ROW_COLS = [c for c in columns("price_clean") if c not in KEYS + ["source_file"]]


def clean_groups(clean_df: pd.DataFrame) -> pd.DataFrame:
    """1 dòng / nhóm khoá: KEYS + row_hash (không phụ thuộc thứ tự dòng)."""
    rows = clean_df[KEYS].copy()
    rows["h"] = row_hash(clean_df, CLEAN_ROW_COLS)
    grouped = rows.sort_values("h").groupby(KEYS, sort=False)["h"].agg("".join).reset_index()
    grouped["row_hash"] = row_hash(grouped, ["h"])
    return grouped[KEYS + ["row_hash"]]


def clean_changes(prev_groups: pd.DataFrame | None, clean_df: pd.DataFrame):
    """Trả về (khoá cần xoá trong kho, dòng cần ghi lại, snapshot nhóm mới)."""
    groups = clean_groups(clean_df)
    d = diff_snapshots(prev_groups, groups, KEYS, [])
    upsert_keys = d[d["op"] != OP_DELETE][KEYS]
    upsert = clean_df.merge(upsert_keys, on=KEYS)[list(clean_df.columns)]
    return d[KEYS], upsert, groups
