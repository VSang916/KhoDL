"""CDC nguồn Winmart: API chỉ trả TRẠNG THÁI HIỆN TẠI (giá, khuyến mãi lúc gọi), không giữ lịch sử.
So snapshot hôm nay với snapshot ngày trước theo khoá (item_no, store_code) để sinh ra lịch sử thay đổi giá.

- Hash trên giá + khuyến mãi. KHÔNG hash stock_quantity (đổi liên tục, sẽ làm mọi sản phẩm thành "thay đổi").
- Sản phẩm vắng mặt chưa xoá ngay: phải vắng DELETE_AFTER_DAYS lần chạy liên tiếp mới thành D
  (API lỗi phân trang, hết hàng tạm thời... không tạo ra D giả).
- Nếu hôm nay lấy được quá ít sản phẩm so với hôm trước (< MIN_COVERAGE) thì coi là lấy thiếu: không đếm vắng mặt.
"""
from __future__ import annotations

import pandas as pd

from pipelines.common.cdc import OP_DELETE, OP_INSERT, diff_snapshots, row_hash

KEYS = ["item_no", "store_code"]
TRACKED = ["original_price", "selling_price", "promotion_code", "is_promo"]
DELETE_AFTER_DAYS = 2
MIN_COVERAGE = 0.8

SNAPSHOT_COLS = KEYS + TRACKED + ["row_hash", "last_seen_date_id", "missing_days"]
SNAPSHOT_DTYPE = {"item_no": str, "promotion_code": str}

CHANGE_COLS = (
    ["change_date_id", "item_no", "store_code", "op", "change_type"]
    + [f"{p}_{c}" for c in TRACKED for p in ("old", "new")]
)


def _change_type(r) -> str:
    if r["op"] == OP_INSERT:
        return "initial" if r["_initial"] else "new_item"  # initial = lần đầu theo dõi, chưa có mốc so sánh
    if r["op"] == OP_DELETE:
        return "removed"
    if r["is_promo_old"] == 0 and r["is_promo_new"] == 1:
        return "promo_start"
    if r["is_promo_old"] == 1 and r["is_promo_new"] == 0:
        return "promo_end"
    if r["selling_price_new"] < r["selling_price_old"]:
        return "price_down"
    if r["selling_price_new"] > r["selling_price_old"]:
        return "price_up"
    return "other"  # đổi giá gốc / mã khuyến mãi mà giá bán không đổi


def compute(prev: pd.DataFrame | None, fact: pd.DataFrame, date_id: int) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """prev: snapshot đã commit (hoặc None). fact: fact_price hôm nay.
    Trả về (changes, snapshot_mới, thống kê)."""
    today = fact[KEYS + TRACKED].copy()
    today["promotion_code"] = today["promotion_code"].fillna("").astype(str)
    today["row_hash"] = row_hash(today, TRACKED)
    today["last_seen_date_id"] = date_id
    today["missing_days"] = 0

    if prev is not None:
        prev = prev.copy()
        prev["promotion_code"] = prev["promotion_code"].fillna("").astype(str)
    initial = prev is None or prev.empty
    d = diff_snapshots(prev, today, KEYS, TRACKED)

    stats = {"initial": initial, "today": len(today), "partial_fetch": False}
    pending = pd.DataFrame(columns=SNAPSHOT_COLS)
    if not initial:
        gone = d[d["op"] == OP_DELETE][KEYS]
        absent = prev.merge(gone, on=KEYS)  # sản phẩm có ở snapshot trước, hôm nay không thấy
        active_before = int((prev["missing_days"] == 0).sum())
        coverage = len(today.merge(prev[KEYS], on=KEYS)) / active_before if active_before else 1.0
        stats["coverage"] = round(coverage, 3)

        if coverage < MIN_COVERAGE:
            stats["partial_fetch"] = True  # lấy thiếu: giữ nguyên, không đếm vắng mặt
            pending = absent
            confirmed = absent.iloc[0:0]
        else:
            absent = absent.assign(missing_days=absent["missing_days"] + 1)
            confirmed = absent[absent["missing_days"] >= DELETE_AFTER_DAYS]
            pending = absent[absent["missing_days"] < DELETE_AFTER_DAYS]
        # chỉ giữ D cho các khoá đã vắng đủ lâu
        d = d[(d["op"] != OP_DELETE) | d.set_index(KEYS).index.isin(confirmed.set_index(KEYS).index)]
        stats["pending_missing"] = len(pending)

    d = d.copy()
    d["_initial"] = initial
    d["change_type"] = d.apply(_change_type, axis=1) if len(d) else pd.Series(dtype=str)
    d["change_date_id"] = date_id
    changes = d.rename(columns={f"{c}_{s}": f"{s}_{c}" for c in TRACKED for s in ("old", "new")})
    changes = changes[CHANGE_COLS].reset_index(drop=True)
    for c in ("old_is_promo", "new_is_promo"):
        changes[c] = pd.to_numeric(changes[c]).astype("Int64")  # CSV ghi "1"/"" thay vì "1.0"

    snapshot = pd.concat([today[SNAPSHOT_COLS], pending[SNAPSHOT_COLS]], ignore_index=True)
    stats.update(changes["change_type"].value_counts().to_dict())
    return changes, snapshot, stats
