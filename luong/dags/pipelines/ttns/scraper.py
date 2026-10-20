"""Cào thitruongnongsan.gov.vn/vn/dulieu_groups.aspx (ASP.NET WebForms).

Trang không có API: mỗi lần chọn bộ lọc là một postback kèm __VIEWSTATE, nên giả lập đúng thao tác trên trình duyệt:
GET trang -> click mục dữ liệu bên trái -> chọn lần lượt 5 ô lọc -> bấm "Xuất excel".
"""
from __future__ import annotations

import re
from html.parser import HTMLParser

import requests

from pipelines.common.http_helper import new_session

CAT_ID = 102  # cat_id trên URL (Rau quả)
ITEM_NO = 6  # mục thứ 6 ở cột trái: "Giá rau quả trong nước (Việt Nam)"
PAGE_URL = f"https://thitruongnongsan.gov.vn/vn/dulieu_groups.aspx?cat_id={CAT_ID}"
LISTBOX = "ctl00$maincontent$Repeater1$ctl{:02d}$Tên_ListBox"
EXPORT_BTN = "ctl00$maincontent$export_excel"
N_FILTERS = 5  # Nhóm SP -> Sản phẩm -> Tên mặt hàng -> Năm -> Tháng: chọn đủ 5 ô thì bảng mới có dữ liệu
XLSX_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class _FormParser(HTMLParser):
    """Lấy các input hidden (__VIEWSTATE...), giá trị option của từng <select>, và các ô danh mục bên trái."""

    def __init__(self):
        super().__init__()
        self.hidden, self.selects, self.menu = {}, {}, []
        self._select = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "input" and a.get("type") == "hidden" and a.get("name"):
            self.hidden[a["name"]] = a.get("value") or ""
        elif tag == "select":
            self._select = a.get("name")
            self.selects[self._select] = []
        elif tag == "option" and self._select:
            self.selects[self._select].append(a.get("value") or "")
        elif tag == "a":
            m = re.search(r"(ctl00\$maincontent\$danhmuc\$ctl\d+\$Lbtn_ten)", a.get("href") or "")
            if m:
                self.menu.append(m.group(1))

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


def _postback(s: requests.Session, form: _FormParser, target: str, extra: dict | None = None) -> _FormParser:
    data = dict(form.hidden)
    data.update(extra or {})
    data["__EVENTTARGET"], data["__EVENTARGUMENT"] = target, ""
    resp = s.post(PAGE_URL, data=data, timeout=120)
    resp.raise_for_status()
    return _parse(resp.text)


def scrape_excel(s: requests.Session, product: str = "") -> bytes:
    """Trả về file .xlsx gốc do nút "Xuất excel" sinh ra. product="" nghĩa là tất cả sản phẩm."""
    resp = s.get(PAGE_URL, timeout=60)
    resp.raise_for_status()
    form = _parse(resp.text)
    if len(form.menu) < ITEM_NO:
        raise ValueError(f"Không tìm thấy mục dữ liệu thứ {ITEM_NO} (trang chỉ có {len(form.menu)} mục) - giao diện đã đổi?")
    form = _postback(s, form, form.menu[ITEM_NO - 1])

    # Chọn từng ô lọc một, mỗi ô một postback như trên trình duyệt (gửi cả 5 ô một lúc server trả 500).
    # Option "Tất cả" có value rỗng.
    chosen = {}
    for k in range(N_FILTERS):
        name = LISTBOX.format(k)
        opts = form.selects.get(name)
        if not opts:
            continue
        value = product if k == 1 else ""
        if value not in opts:
            raise ValueError(f"Không có '{value}' trong ô lọc {name}")
        chosen[name] = value
        form = _postback(s, form, name, chosen)

    data = dict(form.hidden)
    data.update(chosen)
    data["__EVENTTARGET"], data["__EVENTARGUMENT"] = "", ""
    data[EXPORT_BTN] = "Xuất excel"
    resp = s.post(PAGE_URL, data=data, timeout=300)
    resp.raise_for_status()
    if resp.content[:2] != b"PK":  # xlsx là file zip
        raise ValueError(f"Không nhận được file Excel (content-type={resp.headers.get('content-type')})")
    return resp.content
