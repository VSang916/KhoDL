"""Đọc file raw của nguồn thitruongnongsan.gov.vn và đưa về một khuôn chung (khuôn file Excel của trang cũ) để
bước làm sạch (cleaning.py) không phải biết file đến từ trang nào. Hàm thuần, dễ test.

Trang nguonwmy.aspx (từ 11/2026) khác trang cũ dulieu_groups.aspx, bù như sau:
  - file là bảng HTML mã hoá UTF-16 (đuôi .xls), không phải xlsx
  - không có cột Nhóm sản phẩm  -> xuất từng nhóm, gắn nhóm theo file
  - không có cột Sản phẩm       -> suy từ danh sách Sản phẩm của nhóm: tên sản phẩm dài nhất là phần đầu tên mặt hàng
  - không có tỉnh mới (sau sáp nhập 2025) -> bảng TINH_MOI
  - Giá đã là giá trung bình tháng do trang tính sẵn (theo mặt hàng, tỉnh, loại giá, đơn vị, nguồn)
"""
from __future__ import annotations

import io
import json
from html.parser import HTMLParser
from typing import Callable

import pandas as pd

from pipelines.ttns.cleaning import RAW_COLUMNS, _text

# Tiêu đề cột của file xuất (dấu _ thay khoảng trắng) -> tiêu đề tương ứng trong file Excel của trang cũ
EXPORT_COLUMNS = {
    "Tên_mặt_hàng": "Tên_mặt_hàng", "Thị_trường": "Thị_trường", "Loại_giá": "Loại_giá",
    "Đơn_vị_tính": "Đơn_vị_tính", "Nguồn": "Nguồn", "Năm": "Năm", "Tháng": "Tháng", "Giá": "Giá",
}

# Tỉnh cũ -> tỉnh mới theo Nghị quyết sáp nhập 2025 (cùng cách viết với cột "Thị_trường (tên mới)" của trang cũ)
TINH_MOI = {
    "Hà Nội": "TP. Hà Nội", "Sơn La": "Sơn La", "Thái Bình": "Hưng Yên",
    "Long An": "Tây Ninh",
    "Tiền Giang": "Đồng Tháp", "Đồng Tháp": "Đồng Tháp",
    "Bến Tre": "Vĩnh Long", "Trà Vinh": "Vĩnh Long", "Vĩnh Long": "Vĩnh Long",
    "Cần Thơ": "TP. Cần Thơ", "Hậu Giang": "TP. Cần Thơ", "Sóc Trăng": "TP. Cần Thơ",
    "Cà Mau": "Cà Mau", "Bạc Liêu": "Cà Mau",
    "An Giang": "An Giang", "Kiên Giang": "An Giang",
}


class _TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows, self._row, self._cell = [], None, None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = ""

    def handle_data(self, data):
        if self._cell is not None:
            self._cell += data

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None:
            self._row.append(self._cell)
            self._cell = None
        elif tag == "tr" and self._row:
            self.rows.append(self._row)
            self._row = None


def parse_export(content: bytes) -> pd.DataFrame:
    """File .xls của nút "Xuất ra excel" -> DataFrame (mọi cột là chuỗi, giữ nguyên như nguồn)."""
    p = _TableParser()
    p.feed(content.decode("utf-16"))
    if not p.rows:
        raise ValueError("File xuất không có bảng")
    header, body = p.rows[0], p.rows[1:]
    return pd.DataFrame(body, columns=[_text(h) for h in header])


def product_of(item: str, products: list[str]) -> str:
    """'Bưởi da xanh (loại 1)' -> 'Bưởi'; 'Chanh leo tím' -> 'Chanh leo' (khớp dài nhất). Không khớp -> chính nó."""
    key = _text(item).casefold()
    for p in sorted(products, key=len, reverse=True):
        pk = _text(p).casefold()
        if key == pk or key.startswith(pk + " "):
            return _text(p)
    return _text(item)


def to_ttns_raw(export: pd.DataFrame, group: str, products: list[str]) -> pd.DataFrame:
    missing = set(EXPORT_COLUMNS) - set(export.columns)
    if missing:
        raise ValueError(f"File xuất thiếu cột {sorted(missing)} - có thể trang đã đổi cấu trúc")
    df = export[list(EXPORT_COLUMNS)].rename(columns=EXPORT_COLUMNS)
    df["Nhóm sản phẩm"] = group
    df["Sản phẩm"] = df["Tên_mặt_hàng"].map(lambda m: product_of(m, products))
    # Tỉnh chưa có trong bảng để trống: bước làm sạch điền bằng chính tỉnh cũ
    df["Thị_trường (tên mới)"] = df["Thị_trường"].map(lambda t: TINH_MOI.get(_text(t), ""))
    return df[list(RAW_COLUMNS)]


def read_raw(f: dict, get: Callable[[str], bytes]) -> pd.DataFrame:
    """f: 1 file raw như task extract trả về ({"key", "group", "products"}); get(key) -> bytes từ MinIO.
    Trả về DataFrame theo khuôn file Excel của trang cũ (RAW_COLUMNS)."""
    if f["key"].endswith(".xlsx"):  # raw trước 11/2026 (trang dulieu_groups) - vẫn replay được
        return pd.read_excel(io.BytesIO(get(f["key"])))
    return to_ttns_raw(parse_export(get(f["key"])), f["group"], json.loads(get(f["products"])))
