"""Biến đổi file raw Winmart thành star schema (hàm thuần, dễ test)."""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from pipelines.winmart.api import items_of
from pipelines.winmart.schema import SNAPSHOT_TABLES, columns

log = logging.getLogger(__name__)


def _num(v):
    """Số dương hoặc None."""
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _str(v) -> str:
    return "" if v is None else str(v).strip()


def build_tables(docs: list[dict]) -> tuple[dict[str, pd.DataFrame], dict]:
    """docs: danh sách file raw ({"meta":..., "pages":[response API...]}) -> 4 bảng + thống kê."""
    products, markets, dates, facts = {}, {}, {}, {}
    stats = {"items_seen": 0, "skipped_no_price": 0, "sale_gt_price": 0}

    for doc in docs:
        meta = doc["meta"]
        store_code = int(meta["store_code"])
        captured = datetime.fromisoformat(meta["captured_at"])
        date_id = int(captured.strftime("%Y%m%d"))

        markets[store_code] = {
            "store_code": store_code,
            "store_group_code": int(meta["store_group_code"]),
            "market_name": meta["market_name"],
        }
        dates[date_id] = {
            "date_id": date_id,
            "full_date": captured.strftime("%Y-%m-%d"),
            "day": captured.day,
            "month": captured.month,
            "year": captured.year,
        }

        for page in doc["pages"]:
            for it in items_of(page):
                stats["items_seen"] += 1
                item_no = _str(it.get("itemNo"))
                price = _num(it.get("price"))
                if not item_no or not price:
                    stats["skipped_no_price"] += 1
                    log.warning("Bỏ qua (thiếu mã hoặc giá): %s", it.get("name"))
                    continue

                sale = _num(it.get("salePrice"))
                if sale and sale > price:  # bất thường: giá bán cao hơn giá gốc
                    stats["sale_gt_price"] += 1
                    sale = None
                selling = sale or price

                products[item_no] = {
                    "item_no": item_no,
                    "product_name": _str(it.get("name")),
                    "brand_name": _str(it.get("brandName")),
                    "category_name": _str(it.get("categoryName")),
                    "mch3_name": _str(it.get("mch3Name")),
                    "mch4_name": _str(it.get("mch4Name")),
                    "mch5_name": _str(it.get("mch5Name")),
                    "uom_name": _str(it.get("uomName")),
                    "seo_name": _str(it.get("seoName")),
                }
                facts[(item_no, store_code, date_id)] = {
                    "item_no": item_no,
                    "store_code": store_code,
                    "date_id": date_id,
                    "captured_at": captured.strftime("%Y-%m-%d %H:%M:%S"),
                    "original_price": price,
                    "selling_price": selling,
                    "stock_quantity": _num(it.get("quantity")) or 0.0,
                    "promotion_code": _str(it.get("promotionCode")),
                    "promotion_type": _str(it.get("promotionType")),
                    "is_promo": 1 if selling < price else 0,
                }

    rows = {
        "dim_product": sorted(products.values(), key=lambda r: r["item_no"]),
        "dim_market": list(markets.values()),
        "dim_date": sorted(dates.values(), key=lambda r: r["date_id"]),
        "fact_price": list(facts.values()),
    }
    frames = {t: pd.DataFrame(rows[t], columns=columns(t)) for t in SNAPSHOT_TABLES}
    return frames, stats
