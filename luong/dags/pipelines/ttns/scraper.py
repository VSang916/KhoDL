"""Cào thitruongnongsan.gov.vn/vn/nguonwmy.aspx ("Cơ sở dữ liệu giá nông sản", ASP.NET WebForms).

Trang không có API: mỗi lần chọn bộ lọc là một postback kèm __VIEWSTATE, nên giả lập đúng thao tác trên trình duyệt:
GET trang -> chọn Ngành hàng "Rau, quả" -> chọn Nhóm sản phẩm -> điền khoảng ngày, Xem theo "Tháng", tick đủ 4 ô
"Nhóm theo các trường" (loại giá, đơn vị, loại tiền, nguồn) -> Xem -> "Xuất ra excel".
Xuất từng nhóm sản phẩm một vì file xuất không có cột nhóm. Danh sách Sản phẩm của nhóm lấy từ ô lọc Sản_phẩm
(file xuất chỉ có tên mặt hàng) để dựng lại phân cấp sản phẩm -> mặt hàng.

Trước 11/2026 luồng cào trang dulieu_groups.aspx?cat_id=102 (file .xlsx, dữ liệu chỉ đến 12/2025); raw cũ vẫn nằm
trên MinIO và vẫn replay được (xem export.py).
"""
from __future__ import annotations

from html.parser import HTMLParser

import requests

from pipelines.common.http_helper import new_session

PAGE_URL = "https://thitruongnongsan.gov.vn/vn/nguonwmy.aspx"
NGANH = "Rau, quả"
GROUPS = ["Rau củ quả", "Trái cây"]
FROM_DATE = "01/01/2015"  # trang lọc theo khoảng ngày; lấy toàn bộ lịch sử vì CDC so trên toàn bộ
XLS_TYPE = "application/vnd.ms-excel"

F = "ctl00$maincontent$"
FIELDS = {
    "nganh": F + "Ngành_hàng", "nhom": F + "Nhóm_sản_phẩm", "san_pham": F + "Sản_phẩm",
    "tu_ngay": F + "tu_ngay", "den_ngay": F + "den_ngay", "theo": F + "Theo_thời_gian",
}
GROUP_BY = [F + f"hiện_các_trường${i}" for i in range(4)]


class _FormParser(HTMLParser):
    """Lấy các input hidden (__VIEWSTATE...), giá trị option của từng <select> và tên các checkbox."""

    def __init__(self):
        super().__init__()
        self.hidden, self.selects, self.checks = {}, {}, []
        self._select = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "input" and a.get("type") == "hidden" and a.get("name"):
            self.hidden[a["name"]] = a.get("value") or ""
        elif tag == "input" and a.get("type") == "checkbox":
            self.checks.append(a.get("name"))
        elif tag == "select":
            self._select = a.get("name")
            self.selects[self._select] = []
        elif tag == "option" and self._select:
            self.selects[self._select].append(a.get("value") or "")

    def handle_endtag(self, tag):
        if tag == "select":
            self._select = None


def _parse(html: str) -> _FormParser:
    p = _FormParser()
    p.feed(html)
    return p


def session() -> requests.Session:
    # Không retry khi 500: với trang này 500 thường là form gửi sai, retry cũng vô ích
    return new_session(retry_statuses=(502, 503, 504), methods=("GET", "POST"), backoff=2)


def _post(s: requests.Session, form: _FormParser, target: str, data: dict) -> requests.Response:
    body = {**form.hidden, **data, "__EVENTTARGET": target, "__EVENTARGUMENT": ""}
    resp = s.post(PAGE_URL, data=body, timeout=300)
    resp.raise_for_status()
    return resp


def scrape_group(s: requests.Session, group: str, to_date: str) -> tuple[bytes, list[str]]:
    """Trả về (file "excel" gốc của nhóm, danh sách tên Sản phẩm của nhóm). to_date dạng dd/mm/yyyy."""
    resp = s.get(PAGE_URL, timeout=60)
    resp.raise_for_status()
    form = _parse(resp.text)
    data = {FIELDS["tu_ngay"]: FROM_DATE, FIELDS["den_ngay"]: to_date, FIELDS["theo"]: "thang", FIELDS["nganh"]: NGANH}
    form = _parse(_post(s, form, FIELDS["nganh"], data).text)
    if group not in form.selects.get(FIELDS["nhom"], []):
        raise ValueError(f"Không có nhóm '{group}' trong ô lọc - giao diện đã đổi?")
    data[FIELDS["nhom"]] = group
    form = _parse(_post(s, form, FIELDS["nhom"], data).text)
    products = [v for v in form.selects.get(FIELDS["san_pham"], []) if v]
    if sorted(form.checks) != sorted(GROUP_BY):
        raise ValueError(f"Ô 'Nhóm theo các trường' khác mong đợi: {form.checks}")
    data.update({c: "on" for c in GROUP_BY})

    # Bấm Xem trước rồi mới Xuất (như trên trình duyệt); xuất thẳng thì có lúc mất các cột nhóm theo trường
    form = _parse(_post(s, form, "", {**data, F + "Xem": "Xem"}).text)
    resp = _post(s, form, "", {**data, F + "tai_excel": "Xuất ra excel"})
    if not resp.content.startswith(b"\xff\xfe"):  # file .xls thực chất là bảng HTML mã hoá UTF-16
        raise ValueError(f"Không nhận được file xuất (content-type={resp.headers.get('content-type')})")
    return resp.content, products
