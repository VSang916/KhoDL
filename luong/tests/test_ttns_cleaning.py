import pandas as pd

from pipelines.ttns.cleaning import BUILD_TABLES, build_tables, clean
from pipelines.ttns.schema import columns


def _raw(rows):
    cols = ["Nhóm sản phẩm", "Sản phẩm", "Tên_mặt_hàng", "Năm", "Tháng", "Giá", "Thị_trường",
            "Thị_trường (tên mới)", "Nguồn", "Loại_giá", "Đơn_vị_tính"]
    return pd.DataFrame(rows, columns=cols)


ROWS = [
    ["Trái cây", "Sầu riêng", "Sầu riêng Ri6", 2025, 7, 60000, "Tiền Giang", "Đồng Tháp", "CTV", "Thu mua", "Vnđ/Kg"],
    ["Trái cây", "Sầu riêng", "Sầu riêng Ri6", 2025, 7, 60000, "Tiền Giang", "Đồng Tháp", "CTV", "Thu mua", "Vnđ/Kg"],
    ["Trái cây", "Sầu Riêng ", "Sầu riêng Ri6", 2025, 7, 70000, "Tiền Giang", None, "CTV", "Thương lái thu mua", "VNĐ/kg"],
    ["Trái cây", "Sầu riêng", "Sầu riêng Ri6", 2025, 7, 600, "Tiền Giang", "Đồng Tháp", "CTV", "Thu mua", "VNĐ/kg"],
    ["Rau củ quả", "Dưa chuột/dưa chuột", "Dưa chuột/dưa chuột", 2025, 6, 15000, "Hà Nội", "TP. Hà Nội", "CTV", "Bán lẻ", "VNĐ/chục"],
    ["Rau củ quả", "Cà chua", "Cà chua", 2025, 13, 15000, "Hà Nội", "TP. Hà Nội", "CTV", "Bán lẻ", "VNĐ/kg"],
    ["Rau củ quả", "Cà chua", "Cà chua", 2025, 6, 0, "Hà Nội", "TP. Hà Nội", "CTV", "Bán lẻ", "VNĐ/kg"],
]


def test_clean_rules():
    df, stats = clean(_raw(ROWS), "test.xlsx")
    assert stats["dropped_invalid"] == 2  # tháng 13 và giá 0
    assert stats["filled_tinh_moi"] == 1
    assert set(df["san_pham"]) == {"Sầu riêng", "Dưa chuột"}  # gộp hoa thường + khoảng trắng + alias
    assert set(df["don_vi_tinh"]) == {"VNĐ/kg", "VNĐ/chục quả"}
    assert set(df["cap_gia"]) == {"Thu mua", "Bán lẻ"}
    assert (df["tinh_moi"] != "").all()
    assert df.loc[df["gia"] == 600, "is_outlier"].item() == 1  # thiếu 2 số 0
    assert stats["identical_rows_kept"] == 1  # dòng trùng được giữ
    assert list(df.columns) == columns("price_clean")


def test_build_tables_monthly_excludes_outliers():
    df, _ = clean(_raw(ROWS), "test.xlsx")
    frames = build_tables(df)
    assert set(frames) == set(BUILD_TABLES)
    durian = frames["fact_price_monthly"].query("loai_gia == 'Thu mua'").iloc[0]
    assert durian["n_obs"] == 3 and durian["n_outliers"] == 1
    assert durian["avg_price"] == 60000  # giá 600 không được tính
    assert frames["dim_product"]["product_id"].is_unique
    assert frames["dim_date"]["first_day"].tolist() == ["2025-06-01", "2025-07-01"]


def test_ids_stable_between_runs():
    a, _ = clean(_raw(ROWS), "a.xlsx")
    b, _ = clean(_raw(list(reversed(ROWS))), "b.xlsx")
    assert set(a["product_id"]) == set(b["product_id"])


def test_new_dimensions_and_additive_measures():
    df, _ = clean(_raw(ROWS), "test.xlsx")
    frames = build_tables(df)
    pt = frames["dim_price_type"].set_index("loai_gia")
    assert pt.loc["Thương lái thu mua", "cap_gia"] == "Thu mua"          # phân cấp loai_gia -> cap_gia
    assert set(frames["dim_market"]["vung"]) == {"Đồng bằng sông Cửu Long", "Đồng bằng sông Hồng"}
    f = frames["fact_price_monthly"]
    assert ((f["sum_price"] / f["n_valid"]).round(1) == f["avg_price"]).all()   # avg cuộn lên được từ sum / n


def test_olap_cuboid_sql():
    from pipelines.ttns import olap
    assert len(olap.CUBOIDS) == 3
    for name, spec in olap.CUBOIDS.items():
        drop, create, insert = olap.build_sql(name, spec)
        assert "AggregatingMergeTree" in create and "quantileExactState(0.5)" in insert
        # mỗi khối giữ cả các mức cha để cuộn lên không cần join
        cols = olap.cuboid_columns(spec["levels"])
        assert "cap_gia" in cols and "vung" in cols and "year" in cols
