"""Khối con (cuboid) vật chất hoá cho OLAP trên kho ttns - dựng lại toàn bộ sau mỗi lần nạp dữ liệu.

Mạng khối: 4 chiều có phân cấp
    Thời gian : thang -> quy -> nam -> (tất cả)            L = 3
    Sản phẩm  : mat_hang -> san_pham -> nhom -> (tất cả)    L = 3
    Địa điểm  : tinh_cu -> tinh_moi -> vung -> (tất cả)     L = 3
    Loại giá  : loai_gia -> cap_gia -> (tất cả)              L = 2
    => số khối con T = (3+1)(3+1)(3+1)(2+1) = 192  (Bài 5, slide 19)

Không thể vật chất hoá cả 192 khối, nên chọn một phần bằng thuật toán tham lam (Harinarayan-Rajaraman-Ullman,
Bài 5 slide 27-28) trên kích thước thật của từng khối: xem olap/OLAP_GiaNongSan.ipynb mục 2. Ba khối dưới đây là
ba vòng đầu thuật toán chọn; từ vòng 4 lợi ích giảm hẳn.

Độ đo trong khối (tính từ từng lần báo giá hợp lệ, đơn vị VNĐ/kg):
    so_bao_gia, tong_gia : PHÂN PHỐI ĐƯỢC (cộng lên mọi mức)  -> gia_tb = tong_gia / so_bao_gia là ĐẠI SỐ
    gia_min, gia_max     : PHÂN PHỐI ĐƯỢC (min của min, max của max)
    gia_tv (trung vị)    : TOÀN THỂ - không gộp được từ các trung vị con. Lưu TRẠNG THÁI quantileExactState
                           (giữ đủ giá trị) để gộp lại chính xác bằng quantileExactMerge(0.5)(gia_tv).
"""
from __future__ import annotations

DB = "ttns"

# cột của mỗi mức: mức đó + toàn bộ mức cha (phụ thuộc hàm, không làm tăng số dòng) để cuộn lên không cần join
LEVEL_COLS = {
    "thoi_gian": {"thang": ["year", "quarter", "month"], "quy": ["year", "quarter"], "nam": ["year"], None: []},
    "san_pham": {"mat_hang": ["product_id", "nhom_san_pham", "san_pham", "mat_hang"],
                 "san_pham": ["nhom_san_pham", "san_pham"], "nhom": ["nhom_san_pham"], None: []},
    "dia_diem": {"tinh_cu": ["market_id", "tinh_cu", "tinh_moi", "vung"], "tinh_moi": ["tinh_moi", "vung"],
                 "vung": ["vung"], None: []},
    "loai_gia": {"loai_gia": ["loai_gia", "cap_gia"], "cap_gia": ["cap_gia"], None: []},
}
COL_TYPES = {
    "year": "UInt16", "quarter": "UInt8", "month": "UInt8", "product_id": "UInt32", "market_id": "UInt32",
    "nhom_san_pham": "String", "san_pham": "String", "mat_hang": "String", "tinh_cu": "String", "tinh_moi": "String",
    "vung": "String", "loai_gia": "String", "cap_gia": "String",
}
# biểu thức lấy cột từ bảng nguồn (alias c = price_clean, m = dim_market)
COL_EXPR = {
    "year": "c.nam", "quarter": "toUInt8(intDiv(c.thang - 1, 3) + 1)", "month": "c.thang",
    "product_id": "c.product_id", "market_id": "c.market_id", "nhom_san_pham": "c.nhom_san_pham",
    "san_pham": "c.san_pham", "mat_hang": "c.mat_hang", "tinh_cu": "c.tinh_cu", "tinh_moi": "c.tinh_moi",
    "vung": "m.vung", "loai_gia": "c.loai_gia", "cap_gia": "c.cap_gia",
}

# 3 khối con được thuật toán tham lam chọn (thứ tự = thứ tự chọn), kèm lý do nghiệp vụ
CUBOIDS = {
    "cube_quy_mathang_tinh_loaigia": {
        "levels": {"thoi_gian": "quy", "san_pham": "mat_hang", "dia_diem": "tinh_cu", "loai_gia": "loai_gia"},
        "ly_do": "vòng 1, lợi ích lớn nhất: gom 3 tháng -> phục vụ mọi báo cáo theo quý / năm ở mọi mức sản phẩm, địa điểm",
    },
    "cube_thang_nhom_tinh_loaigia": {
        "levels": {"thoi_gian": "thang", "san_pham": "nhom", "dia_diem": "tinh_cu", "loai_gia": "loai_gia"},
        "ly_do": "vòng 2: giữ độ mịn tháng (xu hướng, mùa vụ) nhưng gộp sản phẩm về 2 nhóm -> rất nhỏ (~760 dòng)",
    },
    "cube_nam_mathang_tinh_loaigia": {
        "levels": {"thoi_gian": "nam", "san_pham": "mat_hang", "dia_diem": "tinh_cu", "loai_gia": "loai_gia"},
        "ly_do": "vòng 3: so sánh giá theo năm của từng mặt hàng (bảng xếp hạng, tăng trưởng năm)",
    },
}

MEASURES_DDL = ("so_bao_gia SimpleAggregateFunction(sum, UInt64), tong_gia SimpleAggregateFunction(sum, Float64), "
                "gia_min SimpleAggregateFunction(min, Float64), gia_max SimpleAggregateFunction(max, Float64), "
                "gia_tv AggregateFunction(quantileExact(0.5), Float64)")
MEASURES_SQL = "count(), sum(c.gia), min(c.gia), max(c.gia), quantileExactState(0.5)(c.gia)"
BASE_FROM = (f"FROM {DB}.price_clean AS c "
             f"LEFT JOIN (SELECT market_id, vung FROM {DB}.dim_market FINAL) AS m ON c.market_id = m.market_id "
             "WHERE c.is_outlier = 0 AND c.don_vi_tinh = 'VNĐ/kg'")


def cuboid_columns(levels: dict) -> list[str]:
    cols = []
    for dim, level in levels.items():
        cols += LEVEL_COLS[dim][level]
    return cols


def build_sql(name: str, spec: dict) -> list[str]:
    """Các câu SQL dựng lại 1 khối con (xoá, tạo, nạp)."""
    cols = cuboid_columns(spec["levels"])
    ddl_cols = ", ".join(f"{c} {COL_TYPES[c]}" for c in cols)
    comment = spec["ly_do"].replace("'", "")
    return [
        f"DROP TABLE IF EXISTS {DB}.{name}",
        f"CREATE TABLE {DB}.{name} ({ddl_cols}, {MEASURES_DDL}) "
        f"ENGINE = AggregatingMergeTree ORDER BY ({', '.join(cols)}) COMMENT '{comment}'",
        f"INSERT INTO {DB}.{name} SELECT {', '.join(COL_EXPR[c] for c in cols)}, {MEASURES_SQL} "
        f"{BASE_FROM} GROUP BY {', '.join(COL_EXPR[c] for c in cols)}",
    ]
