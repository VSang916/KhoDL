"""
Tải dữ liệu từ thitruongnongsan.gov.vn/vn/dulieu_groups.aspx (ASP.NET WebForms).

Trang không có API: mỗi lần chọn bộ lọc là một postback kèm __VIEWSTATE.
Script giả lập đúng chuỗi thao tác trên trình duyệt:
  1. GET trang
  2. Click mục dữ liệu bên trái (vd "6. Giá rau quả trong nước")
  3. Chọn lần lượt các ô lọc Nhóm SP -> Sản phẩm -> Tên mặt hàng -> Năm -> Tháng
     (mỗi ô một postback; "Tất cả" có value = "")
  4. Bấm "Xuất excel" -> server trả về file .xlsx

Ví dụ:
  python scrape_ttns.py                       # mục 6, tất cả sản phẩm
  python scrape_ttns.py --product "Vải" --product "Xoài"
  python scrape_ttns.py --item 9              # mục 9: Giá rau quả Trung Quốc
"""
import argparse
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

URL = "https://thitruongnongsan.gov.vn/vn/dulieu_groups.aspx?cat_id={cat}"
LB = "ctl00$maincontent$Repeater1$ctl{:02d}$Tên_ListBox"
EXPORT_BTN = "ctl00$maincontent$export_excel"
N_FILTERS = 5  # Nhóm SP, Sản phẩm, Tên mặt hàng, Năm, Tháng -> đủ để bảng hiện dữ liệu


def hidden_fields(html):
    soup = BeautifulSoup(html, "lxml")
    return soup, {i["name"]: i.get("value", "") for i in soup.select("input[type=hidden]")}


def options(soup, name):
    sel = soup.find("select", attrs={"name": name})
    return [o.get("value", "") for o in sel.select("option")] if sel else []


def open_item(s, url, item):
    """Mở trang và click mục dữ liệu thứ `item` (1-based) ở cột trái."""
    soup, d = hidden_fields(s.get(url, timeout=60).text)
    links = re.findall(r"ctl00\$maincontent\$danhmuc\$ctl(\d+)\$Lbtn_ten", str(soup))
    target = f"ctl00$maincontent$danhmuc$ctl{links[item - 1]}$Lbtn_ten"
    d["__EVENTTARGET"], d["__EVENTARGUMENT"] = target, ""
    return hidden_fields(s.post(url, data=d, timeout=60).text)


def select_filters(s, url, soup, d, wanted):
    """wanted: {index_ô_lọc: value}. Ô không chỉ định = "Tất cả" ("")."""
    chosen = {}
    for k in range(N_FILTERS):
        name = LB.format(k)
        if not options(soup, name):
            continue
        val = wanted.get(k, "")
        if val not in options(soup, name):
            raise ValueError(f"'{val}' không có trong ô lọc {k}")
        chosen[name] = val
        d.update(chosen)
        d["__EVENTTARGET"], d["__EVENTARGUMENT"] = name, ""
        r = s.post(url, data=d, timeout=120)
        r.raise_for_status()
        soup, d = hidden_fields(r.text)
    return soup, d, chosen


def export_excel(s, url, d, chosen, out):
    d.update(chosen)
    d["__EVENTTARGET"], d["__EVENTARGUMENT"] = "", ""
    d[EXPORT_BTN] = "Xuất excel"
    r = s.post(url, data=d, timeout=300)
    r.raise_for_status()
    if r.content[:2] != b"PK":  # xlsx là file zip
        raise RuntimeError(f"Không nhận được file Excel (content-type={r.headers.get('content-type')})")
    out.write_bytes(r.content)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cat", type=int, default=102, help="cat_id trên URL")
    ap.add_argument("--item", type=int, default=6, help="số thứ tự mục dữ liệu ở cột trái")
    ap.add_argument("--product", action="append", help="tên sản phẩm; bỏ trống = tất cả")
    ap.add_argument("--out", default=str(Path(__file__).parent / "raw"))
    a = ap.parse_args()

    url = URL.format(cat=a.cat)
    outdir = Path(a.out)
    outdir.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"

    for prod in a.product or [""]:
        soup, d = open_item(s, url, a.item)
        soup, d, chosen = select_filters(s, url, soup, d, {1: prod})
        fname = f"cat{a.cat}_item{a.item}_{prod or 'all'}.xlsx".replace(" ", "_")
        p = export_excel(s, url, d, chosen, outdir / fname)
        print(f"OK {p} ({p.stat().st_size:,} bytes)")
        time.sleep(1)  # lịch sự với server


if __name__ == "__main__":
    main()
