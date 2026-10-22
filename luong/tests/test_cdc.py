import pandas as pd

from pipelines.common.cdc import diff_snapshots, row_hash, scope_name
from pipelines.ttns import cdc as ttns_cdc
from pipelines.winmart import cdc as winmart_cdc


# ================================================================ common
def _snap(rows, cols=("k", "v")):
    df = pd.DataFrame(rows, columns=list(cols))
    df["row_hash"] = row_hash(df, ["v"])
    return df


def test_diff_snapshots_ops():
    old = _snap([(1, 10), (2, 20), (3, 30)])
    new = _snap([(1, 10.0), (2, 25), (4, 40)])  # 10 vs 10.0 không được coi là thay đổi
    d = diff_snapshots(old, new, ["k"], ["v"]).set_index("k")
    assert d["op"].to_dict() == {2: "U", 3: "D", 4: "I"}
    assert d.loc[2, "v_old"] == 20 and d.loc[2, "v_new"] == 25


def test_diff_without_base_is_all_insert():
    d = diff_snapshots(None, _snap([(1, 10), (2, 20)]), ["k"], ["v"])
    assert set(d["op"]) == {"I"}


def test_scope_name():
    assert scope_name([]) == "all"
    assert scope_name(["Xoài", "Vải"]) == scope_name(["Vải", "Xoài"])


# ================================================================ winmart
def _fact(rows):
    return pd.DataFrame(rows, columns=["item_no", "store_code", "original_price", "selling_price", "promotion_code", "is_promo"])


def _roundtrip(df):
    """Giả lập ghi snapshot ra CSV rồi đọc lại như trên MinIO."""
    import io
    return pd.read_csv(io.StringIO(df.to_csv(index=False)), dtype=winmart_cdc.SNAPSHOT_DTYPE,
                       keep_default_na=False, na_values=[""])


def test_winmart_initial_then_changes():
    day1 = _fact([("A", 1535, 20000, 20000, "", 0), ("B", 1535, 30000, 30000, "", 0), ("C", 1535, 9000, 9000, "", 0),
                  ("D", 1535, 5000, 5000, "", 0), ("E", 1535, 7000, 7000, "", 0)])
    ch1, snap1, st1 = winmart_cdc.compute(None, day1, 20261013)
    assert st1["initial"] and set(ch1["change_type"]) == {"initial"} and len(snap1) == 5

    day2 = _fact([("A", 1535, 20000, 15000, "KM1", 1),  # bắt đầu khuyến mãi
                  ("B", 1535, 32000, 32000, "", 0),     # tăng giá
                  ("D", 1535, 5000, 5000, "", 0), ("E", 1535, 7000, 7000, "", 0),
                  ("F", 1535, 12000, 12000, "", 0)])    # sản phẩm mới; C vắng mặt lần 1
    ch2, snap2, st2 = winmart_cdc.compute(_roundtrip(snap1), day2, 20261014)
    types = dict(zip(ch2["item_no"], ch2["change_type"]))
    assert types == {"A": "promo_start", "B": "price_up", "F": "new_item"}  # C chưa bị xoá
    assert st2["pending_missing"] == 1

    ch3, snap3, _ = winmart_cdc.compute(_roundtrip(snap2), day2, 20261015)  # C vắng lần 2 -> D
    assert dict(zip(ch3["item_no"], ch3["change_type"])) == {"C": "removed"}
    assert "C" not in set(snap3["item_no"])


def test_winmart_partial_fetch_does_not_delete():
    day1 = _fact([(x, 1535, 1000, 1000, "", 0) for x in "ABCDEFGHIJ"])
    _, snap1, _ = winmart_cdc.compute(None, day1, 20261013)
    day2 = day1.head(3)  # API lỗi, chỉ lấy được 3/10
    ch2, snap2, st2 = winmart_cdc.compute(_roundtrip(snap1), day2, 20261014)
    assert st2["partial_fetch"] and len(ch2) == 0
    assert (snap2["missing_days"] == 0).all() and len(snap2) == 10


# ================================================================ ttns
def _monthly(rows):
    cols = ["product_id", "market_id", "date_id", "loai_gia", "cap_gia", "don_vi_tinh", "n_obs", "n_outliers",
            "n_valid", "sum_price", "avg_price", "median_price", "min_price", "max_price"]
    return pd.DataFrame([r[:6] + (r[6], 0, r[6], r[6] * r[7], r[7], r[7], r[7], r[7]) for r in rows], columns=cols)


def test_ttns_change_kinds_and_upserts():
    base = _monthly([(1, 9, 202506, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 4, 20000),
                     (1, 9, 202507, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 4, 22000),
                     (2, 9, 202507, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 2, 9000)])
    _, _, snap, st0 = ttns_cdc.compute(None, base, "2026-10-01", "all", 1)
    assert st0["initial"] == 3

    new = _monthly([(1, 9, 202506, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 5, 21000),   # sửa tháng cũ
                    (1, 9, 202507, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 4, 22000),   # không đổi
                    (3, 9, 202507, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 1, 50000),   # thêm vào tháng đã có
                    (1, 9, 202508, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 3, 25000)])  # tháng mới; khoá (2,...) mất
    log, upsert, snap2, st = ttns_cdc.compute(snap, new, "2026-10-08", "all", 2)
    kinds = {(r.product_id, r.date_id): r.change_kind for r in log.itertuples()}
    assert kinds == {(1, 202506): "revision", (3, 202507): "backfill", (1, 202508): "new_month", (2, 202507): "removed"}
    assert st["delete_ratio"] == round(1 / 3, 4)

    assert len(upsert) == 4 and set(upsert["version"]) == {2}
    deleted = upsert[upsert["is_deleted"] == 1]
    assert deleted["product_id"].tolist() == [2] and deleted["avg_price"].item() == 9000  # D mang giá trị cũ
    rev = log[log["change_kind"] == "revision"].iloc[0]
    assert rev["old_avg_price"] == 20000 and rev["new_avg_price"] == 21000
    assert len(snap2) == 4


def test_ttns_price_clean_groups_include_outlier_only_groups():
    from pipelines.ttns.cleaning import build_tables, clean
    from test_ttns_cleaning import ROWS, _raw

    rows = ROWS + [["Trái cây", "Bơ", "Bơ 034", 2025, 7, 500, "Tiền Giang", "Đồng Tháp", "CTV", "Bán lẻ", "VNĐ/quả"]]
    rows += [["Trái cây", "Bơ", "Bơ 034", 2025, 7, 50000, "Tiền Giang", "Đồng Tháp", "CTV", "Bán buôn", "VNĐ/quả"]] * 2
    df, _ = clean(_raw(rows), "t.xlsx")
    keys, upsert, groups = ttns_cdc.clean_changes(None, df)
    assert len(upsert) == len(df)  # lần đầu: ghi lại toàn bộ, kể cả nhóm không có trong fact
    assert len(groups) >= len(build_tables(df)["fact_price_monthly"])

    changed = df.copy()
    changed.loc[changed["gia"] == 15000, "gia"] = 16000  # sửa 1 dòng dưa chuột
    keys2, upsert2, _ = ttns_cdc.clean_changes(groups, changed)
    assert len(keys2) == 1 and set(upsert2["san_pham"]) == {"Dưa chuột"}
    keys3, upsert3, _ = ttns_cdc.clean_changes(groups, df.sample(frac=1, random_state=1))  # đảo thứ tự dòng
    assert len(keys3) == 0 and len(upsert3) == 0


def test_ttns_old_schema_snapshot_is_accepted():
    """Snapshot mốc ghi trước khi bảng fact có thêm cột (n_valid, sum_price) vẫn so được, kể cả khi có khoá bị xoá."""
    rows = [(1, 9, 202506, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 4, 20000), (2, 9, 202506, "Bán lẻ", "Bán lẻ", "VNĐ/kg", 2, 9000)]
    _, _, snap, _ = ttns_cdc.compute(None, _monthly(rows), "2026-10-01", "all", 1)
    old = snap.drop(columns=["n_valid", "sum_price"])                     # snapshot định dạng cũ
    log, upsert, _, st = ttns_cdc.compute(old, _monthly(rows[:1]), "2026-10-08", "all", 2)
    assert list(log["change_kind"]) == ["removed"] and upsert["is_deleted"].tolist() == [1]
