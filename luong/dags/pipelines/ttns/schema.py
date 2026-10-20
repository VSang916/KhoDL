"""Bảng ClickHouse của nguồn thitruongnongsan.gov.vn (database ttns)."""
from __future__ import annotations

from pipelines.common.clickhouse_helper import columns as _columns

DB = "ttns"

# Cột kỹ thuật cho CDC, không sinh ra từ bước làm sạch
CDC_COLS = ("version", "is_deleted")

TABLES = {
    "dim_product": (
        "product_id UInt32, nhom_san_pham String, san_pham String, mat_hang String",
        "ENGINE = ReplacingMergeTree ORDER BY product_id",
    ),
    "dim_market": (
        "market_id UInt32, tinh_cu String, tinh_moi String",
        "ENGINE = ReplacingMergeTree ORDER BY market_id",
    ),
    "dim_date": (
        "date_id UInt32, first_day Date, year UInt16, quarter UInt8, month UInt8",
        "ENGINE = ReplacingMergeTree ORDER BY date_id",
    ),
    "price_clean": (
        "product_id UInt32, market_id UInt32, date_id UInt32, nhom_san_pham String, san_pham String, "
        "mat_hang String, nam UInt16, thang UInt8, tinh_cu String, tinh_moi String, loai_gia String, "
        "cap_gia String, don_vi_tinh String, gia Float64, is_outlier UInt8, nguon String, source_file String",
        "ENGINE = MergeTree ORDER BY (product_id, market_id, date_id, loai_gia, don_vi_tinh)",
    ),
    # CDC sink: mỗi lần chạy chỉ INSERT dòng thay đổi; ClickHouse giữ bản có version lớn nhất,
    # dòng is_deleted=1 bị ẩn khi đọc FINAL.
    "fact_price_monthly": (
        "product_id UInt32, market_id UInt32, date_id UInt32, loai_gia String, cap_gia String, "
        "don_vi_tinh String, n_obs UInt32, n_outliers UInt32, avg_price Float64, median_price Float64, "
        "min_price Float64, max_price Float64, version UInt64, is_deleted UInt8",
        "ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY (product_id, market_id, date_id, loai_gia, don_vi_tinh)",
    ),
    # Nhật ký thay đổi: mỗi dòng là 1 khoá (mặt hàng, tỉnh, tháng, loại giá, đơn vị) bị thêm / sửa / xoá ở nguồn
    "cdc_log": (
        "run_date Date, scope String, product_id UInt32, market_id UInt32, date_id UInt32, loai_gia String, "
        "don_vi_tinh String, op String, change_kind String, "
        "old_n_obs Nullable(UInt32), new_n_obs Nullable(UInt32), "
        "old_avg_price Nullable(Float64), new_avg_price Nullable(Float64), "
        "old_median_price Nullable(Float64), new_median_price Nullable(Float64)",
        "ENGINE = MergeTree ORDER BY (run_date, product_id, market_id, date_id)",
    ),
}

VIEWS = {
    "v_price_monthly": f"""CREATE VIEW {DB}.v_price_monthly AS
SELECT d.first_day AS month, d.year, d.quarter, d.month AS month_no,
       p.nhom_san_pham, p.san_pham, p.mat_hang, m.tinh_cu, m.tinh_moi,
       f.loai_gia, f.cap_gia, f.don_vi_tinh,
       f.avg_price, f.median_price, f.min_price, f.max_price, f.n_obs, f.n_outliers
FROM {DB}.fact_price_monthly AS f FINAL
LEFT JOIN (SELECT * FROM {DB}.dim_product FINAL) AS p ON f.product_id = p.product_id
LEFT JOIN (SELECT * FROM {DB}.dim_market FINAL) AS m ON f.market_id = m.market_id
LEFT JOIN (SELECT * FROM {DB}.dim_date FINAL) AS d ON f.date_id = d.date_id
WHERE f.is_deleted = 0""",
    "v_changes": f"""CREATE VIEW {DB}.v_changes AS
SELECT c.run_date, c.op, c.change_kind, d.first_day AS month, p.san_pham, p.mat_hang, m.tinh_moi,
       c.loai_gia, c.don_vi_tinh, c.old_avg_price, c.new_avg_price,
       round((c.new_avg_price - c.old_avg_price) / c.old_avg_price * 100, 1) AS pct_change,
       c.old_n_obs, c.new_n_obs, c.scope
FROM {DB}.cdc_log AS c
LEFT JOIN (SELECT * FROM {DB}.dim_product FINAL) AS p ON c.product_id = p.product_id
LEFT JOIN (SELECT * FROM {DB}.dim_market FINAL) AS m ON c.market_id = m.market_id
LEFT JOIN (SELECT * FROM {DB}.dim_date FINAL) AS d ON c.date_id = d.date_id""",
}

# Nguồn trả toàn bộ lịch sử nên các bảng dữ liệu nạp lại được hết; riêng cdc_log là lịch sử do mình tạo ra
REBUILDABLE = ("dim_product", "dim_market", "dim_date", "price_clean", "fact_price_monthly")


def columns(table: str, with_cdc: bool = True) -> list[str]:
    cols = _columns(TABLES, table)
    return cols if with_cdc else [c for c in cols if c not in CDC_COLS]
