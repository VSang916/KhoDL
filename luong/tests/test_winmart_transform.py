from pipelines.winmart.schema import SNAPSHOT_TABLES
from pipelines.winmart.transform import build_tables


def _doc(items):
    return {
        "meta": {"store_code": "1535", "store_group_code": "1998", "market_name": "WinMart 1535",
                 "captured_at": "2026-10-18T07:00:00"},
        "pages": [{"data": {"items": items}}],
    }


def test_build_tables():
    items = [
        {"itemNo": "A1", "name": "Cải ngọt", "price": 20000, "salePrice": 15000, "quantity": 5},
        {"itemNo": "A2", "name": "Cà chua", "price": 30000, "salePrice": 0},
        {"itemNo": "A3", "name": "Lỗi giá", "price": 10000, "salePrice": 12000},  # sale > price -> bỏ sale
        {"itemNo": "A4", "name": "Không giá", "price": None},
    ]
    frames, stats = build_tables([_doc(items)])
    assert set(frames) == set(SNAPSHOT_TABLES)
    assert stats == {"items_seen": 4, "skipped_no_price": 1, "sale_gt_price": 1}

    fact = frames["fact_price"].set_index("item_no")
    assert fact.loc["A1", "selling_price"] == 15000 and fact.loc["A1", "is_promo"] == 1
    assert fact.loc["A2", "selling_price"] == 30000 and fact.loc["A2", "is_promo"] == 0
    assert fact.loc["A3", "selling_price"] == 10000
    assert fact["date_id"].unique().tolist() == [20261018]


def test_same_item_twice_is_deduplicated():
    item = {"itemNo": "A1", "name": "Cải", "price": 20000}
    frames, _ = build_tables([_doc([item, item])])
    assert len(frames["fact_price"]) == 1
