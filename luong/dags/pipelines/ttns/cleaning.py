"""Làm sạch file Excel giá rau quả trong nước và dựng star schema (hàm thuần, dễ test)."""
from __future__ import annotations

import re
import unicodedata
import zlib

import pandas as pd

from pipelines.ttns.schema import columns

BUILD_TABLES = ["dim_product", "dim_market", "dim_date", "price_clean", "fact_price_monthly"]

# Tiêu đề cột trong file Excel -> tên cột sau khi làm sạch
RAW_COLUMNS = {
    "Nhóm sản phẩm": "nhom_san_pham",
    "Sản phẩm": "san_pham",
    "Tên_mặt_hàng": "mat_hang",
    "Năm": "nam",
    "Tháng": "thang",
    "Giá": "gia",
    "Thị_trường": "tinh_cu",
    "Thị_trường (tên mới)": "tinh_moi",
    "Nguồn": "nguon",
    "Loại_giá": "loai_gia",
    "Đơn_vị_tính": "don_vi_tinh",
}

# Gộp tên đồng nghĩa / gõ sai (so khớp không phân biệt hoa thường). Bổ sung khi phát hiện thêm.
NAME_ALIASES = {
    "dưa chuột/dưa chuột": "Dưa chuột",
    "cốc thái": "Cóc Thái",
    "rau mùng tơi": "Mồng tơi",
    "trái dứa/thơm": "Dứa",
    "quả mơ": "Mơ",
    "quả bầu": "Bầu",
}

# Gom loại giá thành 3 cấp để so sánh: giá tới tay người tiêu dùng / giá sỉ / giá tại nơi sản xuất
PRICE_LEVEL = {
    "Bán lẻ": "Bán lẻ",
    "Tại chợ": "Bán lẻ",
    "Bán buôn": "Bán buôn",
    "Tại vựa": "Bán buôn",
    "Thu mua": "Thu mua",
    "Thương lái thu mua": "Thu mua",
    "Tại vườn": "Thu mua",
}

# Giá lệch quá OUTLIER_RATIO lần so với trung vị của (mặt hàng, đơn vị, cấp giá) -> nghi nhập sai (thừa/thiếu số 0)
OUTLIER_RATIO = 5


def _text(v) -> str:
    """Chuẩn hoá Unicode NFC (dữ liệu tiếng Việt hay lẫn NFD), bỏ khoảng trắng thừa."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", str(v))).strip()


def _canonical_names(s: pd.Series) -> pd.Series:
    """Gộp các biến thể chỉ khác hoa thường (vd 'Sầu Riêng'/'Sầu riêng') về cách viết phổ biến nhất, rồi áp alias."""
    key = s.str.casefold()
    most_common = s.groupby(key).agg(lambda x: x.value_counts().index[0])
    out = key.map(most_common)
    return out.map(lambda v: NAME_ALIASES.get(v.casefold(), v))


def _unit(v: str) -> str:
    """'Vnđ/Kg', 'VNĐ/kg' -> 'VNĐ/kg'; 'VNĐ/chục' -> 'VNĐ/chục quả'."""
    rest = v.split("/", 1)[1].strip().lower() if "/" in v else v.lower()
    return "VNĐ/" + ("chục quả" if rest == "chục" else rest)


def _id(*parts: str) -> int:
    """Khoá ổn định giữa các lần chạy (không phụ thuộc thứ tự dòng)."""
    return zlib.crc32("|".join(parts).encode("utf-8"))


def clean(raw: pd.DataFrame, source_file: str) -> tuple[pd.DataFrame, dict]:
    raw = raw.rename(columns=lambda c: _text(c))
    missing = set(RAW_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"File thiếu cột {sorted(missing)} - có thể trang đã đổi cấu trúc")
    df = raw[list(RAW_COLUMNS)].rename(columns=RAW_COLUMNS)
    stats = {"rows_raw": len(df)}

    for c in ("nhom_san_pham", "san_pham", "mat_hang", "tinh_cu", "tinh_moi", "nguon", "loai_gia", "don_vi_tinh"):
        df[c] = df[c].map(_text)
    df["gia"] = pd.to_numeric(df["gia"], errors="coerce")
    df["nam"] = pd.to_numeric(df["nam"], errors="coerce")
    df["thang"] = pd.to_numeric(df["thang"], errors="coerce")

    bad = (
        df["gia"].isna() | (df["gia"] <= 0)
        | ~df["nam"].between(2000, 2100) | ~df["thang"].between(1, 12)
        | (df["san_pham"] == "") | (df["mat_hang"] == "") | (df["tinh_cu"] == "")
    )
    stats["dropped_invalid"] = int(bad.sum())
    df = df[~bad].copy()
    df["nam"] = df["nam"].astype(int)
    df["thang"] = df["thang"].astype(int)

    df["san_pham"] = _canonical_names(df["san_pham"])
    df["mat_hang"] = _canonical_names(df["mat_hang"])
    df["don_vi_tinh"] = df["don_vi_tinh"].map(_unit)
    df["cap_gia"] = df["loai_gia"].map(PRICE_LEVEL).fillna(df["loai_gia"])

    # Tỉnh mới (sau sáp nhập 2025) bị trống ở một số dòng: suy ra từ các dòng khác cùng tỉnh cũ
    known = df[df["tinh_moi"] != ""].groupby("tinh_cu")["tinh_moi"].agg(lambda x: x.value_counts().index[0])
    empty = df["tinh_moi"] == ""
    stats["filled_tinh_moi"] = int(empty.sum())
    df.loc[empty, "tinh_moi"] = df.loc[empty, "tinh_cu"].map(known).fillna(df.loc[empty, "tinh_cu"])

    median = df.groupby(["mat_hang", "don_vi_tinh", "cap_gia"])["gia"].transform("median")
    ratio = df["gia"] / median
    df["is_outlier"] = ((ratio > OUTLIER_RATIO) | (ratio < 1 / OUTLIER_RATIO)).astype(int)
    stats["outliers"] = int(df["is_outlier"].sum())

    df["product_id"] = [_id(a, b, c) for a, b, c in zip(df["nhom_san_pham"], df["san_pham"], df["mat_hang"])]
    df["market_id"] = df["tinh_cu"].map(lambda t: _id(t))
    df["date_id"] = df["nam"] * 100 + df["thang"]  # YYYYMM
    df["source_file"] = source_file
    # Các dòng giống hệt nhau được GIỮ LẠI: trang không có cột ngày nên đó là các lần báo giá khác nhau trong tháng
    stats["identical_rows_kept"] = int(df.duplicated(subset=columns("price_clean")[:-1]).sum())
    stats["rows_clean"] = len(df)
    return df[columns("price_clean")].reset_index(drop=True), stats


def build_tables(clean_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    dim_product = clean_df.drop_duplicates("product_id")[columns("dim_product")].sort_values("product_id")
    dim_market = clean_df.drop_duplicates("market_id")[columns("dim_market")].sort_values("market_id")

    dates = clean_df.drop_duplicates("date_id")[["date_id", "nam", "thang"]].sort_values("date_id")
    dim_date = pd.DataFrame({
        "date_id": dates["date_id"],
        "first_day": [f"{y:04d}-{m:02d}-01" for y, m in zip(dates["nam"], dates["thang"])],
        "year": dates["nam"],
        "quarter": (dates["thang"] - 1) // 3 + 1,
        "month": dates["thang"],
    })

    keys = ["product_id", "market_id", "date_id", "loai_gia", "cap_gia", "don_vi_tinh"]
    counts = clean_df.groupby(keys).agg(n_obs=("gia", "size"), n_outliers=("is_outlier", "sum")).reset_index()
    prices = (
        clean_df[clean_df["is_outlier"] == 0]
        .groupby(keys)["gia"]
        .agg(avg_price="mean", median_price="median", min_price="min", max_price="max")
        .reset_index()
    )
    # inner join: nhóm chỉ toàn giá bất thường thì không có giá tin cậy, bỏ khỏi bảng fact
    fact = counts.merge(prices, on=keys, how="inner")
    fact["avg_price"] = fact["avg_price"].round(1)

    frames = {
        "dim_product": dim_product,
        "dim_market": dim_market,
        "dim_date": dim_date,
        "price_clean": clean_df,
        "fact_price_monthly": fact,
    }
    return {t: frames[t][columns(t, with_cdc=False)].reset_index(drop=True) for t in BUILD_TABLES}
