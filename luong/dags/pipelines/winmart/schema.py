"""Bảng ClickHouse của nguồn Winmart (database winmart)."""
from __future__ import annotations

from pipelines.common.clickhouse_helper import columns as _columns

DB = "winmart"

TABLES = {
    "dim_product": (
        "item_no String, product_name String, brand_name String, category_name String, "
        "mch3_name String, mch4_name String, mch5_name String, uom_name String, seo_name String",
        "ENGINE = ReplacingMergeTree ORDER BY item_no",
    ),
    "dim_market": (
        "store_code UInt32, store_group_code UInt32, market_name String",
        "ENGINE = ReplacingMergeTree ORDER BY store_code",
    ),
    "dim_date": (
        "date_id UInt32, full_date Date, day UInt8, month UInt8, year UInt16",
        "ENGINE = ReplacingMergeTree ORDER BY date_id",
    ),
    "fact_price": (
        "item_no String, store_code UInt32, date_id UInt32, captured_at DateTime('Asia/Ho_Chi_Minh'), "
        "original_price Float64, selling_price Float64, stock_quantity Float64, "
        "promotion_code String, promotion_type String, is_promo UInt8",
        "ENGINE = MergeTree PARTITION BY date_id ORDER BY (item_no, store_code)",
    ),
    # CDC: mỗi dòng là 1 thay đổi trạng thái giá của 1 sản phẩm (I mới / U đổi / D ngừng bán)
    "price_changes": (
        "change_date_id UInt32, item_no String, store_code UInt32, op String, change_type String, "
        "old_original_price Nullable(Float64), new_original_price Nullable(Float64), "
        "old_selling_price Nullable(Float64), new_selling_price Nullable(Float64), "
        "old_promotion_code Nullable(String), new_promotion_code Nullable(String), "
        "old_is_promo Nullable(UInt8), new_is_promo Nullable(UInt8)",
        "ENGINE = MergeTree ORDER BY (item_no, store_code, change_date_id)",
    ),
}

VIEWS = {
    "v_price_daily": f"""CREATE VIEW {DB}.v_price_daily AS
SELECT d.full_date AS date, f.item_no, p.product_name, p.brand_name, p.category_name,
       p.mch4_name, p.mch5_name, p.uom_name, m.market_name, f.store_code,
       f.original_price, f.selling_price, f.is_promo,
       round((f.original_price - f.selling_price) / f.original_price * 100, 1) AS discount_pct,
       f.stock_quantity, f.captured_at
FROM {DB}.fact_price AS f
LEFT JOIN (SELECT * FROM {DB}.dim_product FINAL) AS p ON f.item_no = p.item_no
LEFT JOIN (SELECT * FROM {DB}.dim_market FINAL) AS m ON f.store_code = m.store_code
LEFT JOIN (SELECT * FROM {DB}.dim_date FINAL) AS d ON f.date_id = d.date_id""",
    # SCD loại 2 dựng từ price_changes: mỗi mức giá có valid_from / valid_to (NULL = đang áp dụng)
    "v_price_history": f"""CREATE VIEW {DB}.v_price_history AS
SELECT h.item_no, p.product_name, h.store_code, h.change_type, h.valid_from,
       if(h.next_from = toDate(0), NULL, h.next_from) AS valid_to,
       h.next_from = toDate(0) AS is_current,
       h.selling_price, h.original_price, h.is_promo, h.promotion_code
FROM (
    SELECT item_no, store_code, op, change_type, YYYYMMDDToDate(change_date_id) AS valid_from,
           new_selling_price AS selling_price, new_original_price AS original_price,
           new_is_promo AS is_promo, new_promotion_code AS promotion_code,
           leadInFrame(YYYYMMDDToDate(change_date_id)) OVER (
               PARTITION BY item_no, store_code ORDER BY change_date_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_from
    FROM {DB}.price_changes
) AS h
LEFT JOIN (SELECT item_no, product_name FROM {DB}.dim_product FINAL) AS p ON h.item_no = p.item_no
WHERE h.op != 'D'""",
}

SNAPSHOT_TABLES = ["dim_product", "dim_market", "dim_date", "fact_price"]  # bảng sinh ra từ transform


def columns(table: str) -> list[str]:
    return _columns(TABLES, table)
