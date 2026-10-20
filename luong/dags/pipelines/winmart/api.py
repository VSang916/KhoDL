"""Gọi API category của Winmart (JSON, phân trang)."""
from __future__ import annotations

import logging
import time

import requests

from pipelines.common.http_helper import new_session

log = logging.getLogger(__name__)

API_URL = "https://api-crownx.winmart.vn/it/api/web/v3/item/category"
MAX_PAGES = 200

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Origin": "https://winmart.vn",
    "Referer": "https://winmart.vn/",
}


def session() -> requests.Session:
    return new_session(HEADERS)


def items_of(page: dict) -> list[dict]:
    return (page.get("data") or {}).get("items") or []


def fetch_category(s: requests.Session, store: dict, slug: str, page_size: int) -> list[dict]:
    """Lấy tất cả các trang của 1 danh mục. Trả về danh sách response gốc (mỗi trang 1 phần tử).

    Dừng khi trang trả về rỗng hoặc không có sản phẩm mới (phòng trường hợp server giới hạn pageSize
    hoặc bỏ qua pageNumber), nên không dựa vào việc "số item < pageSize".
    """
    pages, seen = [], set()
    for page in range(1, MAX_PAGES + 1):
        params = {
            "pageNumber": page,
            "pageSize": page_size,
            "slug": slug,
            "storeCode": store["store_code"],
            "storeGroupCode": store["store_group_code"],
        }
        resp = s.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        try:
            body = resp.json()
        except ValueError:
            raise ValueError(f"API không trả JSON (có thể bị chặn). Đầu phản hồi: {resp.text[:200]!r}")

        items = items_of(body)
        new_ids = [i.get("itemNo") for i in items if i.get("itemNo") not in seen]
        log.info("store=%s slug=%s trang %d: %d item (%d mới)", store["store_code"], slug, page, len(items), len(new_ids))
        if not new_ids:
            break
        seen.update(new_ids)
        pages.append(body)
        time.sleep(0.5)  # lịch sự với server
    else:
        log.warning("Đạt MAX_PAGES=%d cho %s, có thể còn dữ liệu", MAX_PAGES, slug)
    return pages
