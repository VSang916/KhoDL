from pipelines.ttns.cleaning import build_tables, clean
from pipelines.ttns.export import parse_export, product_of, to_ttns_raw

HEADER = ["Tên_mặt_hàng", "Thị_trường", "Loại_giá", "Đơn_vị_tính", "Loại_tiền", "Nguồn", "Năm", "Tháng", "Giá"]
ROWS = [
    ["Bưởi da xanh (loại 1)", "Tiền Giang", "Thu mua tại vườn", "VNĐ/Kg", "VNĐ", "CTV địa phương", "2026", "9", "30000"],
    ["Chanh leo tím", "Sơn La", "Bán lẻ", "VNĐ/kg", "VNĐ", "CTV địa phương", "2026", "9", "25000"],
    ["Nho", "Hà Nội", "Bán lẻ", "VNĐ/kg", "VNĐ", "CTV địa phương", "2026", "8", "60000"],
]
PRODUCTS = ["Bưởi", "Chanh", "Chanh leo", "Táo"]


def _export(rows) -> bytes:
    """Giống file của nút "Xuất ra excel": bảng HTML, mã hoá UTF-16 có BOM, tiêu đề dùng thực thể HTML."""
    th = "".join(f'<th scope="col">{h.replace("á", "&#225;")}</th>' for h in HEADER)
    trs = "".join("<tr>" + "".join(f'<td class="textmode">{v}</td>' for v in r) + "</tr>" for r in rows)
    return f"<div><table><tr>{th}</tr>{trs}</table></div>".encode("utf-16")


def test_parse_export():
    df = parse_export(_export(ROWS))
    assert list(df.columns) == HEADER  # thực thể &#225; được giải mã
    assert len(df) == 3 and df.loc[0, "Giá"] == "30000"


def test_product_of_longest_prefix():
    assert product_of("Bưởi da xanh (loại 1)", PRODUCTS) == "Bưởi"
    assert product_of("Chanh leo tím", PRODUCTS) == "Chanh leo"  # khớp dài nhất, không phải "Chanh"
    assert product_of("Chanh", PRODUCTS) == "Chanh"
    assert product_of("Táo mèo", PRODUCTS) == "Táo"
    assert product_of("Tàu hũ", PRODUCTS) == "Tàu hũ"  # không khớp -> chính nó
    assert product_of("Nho", PRODUCTS) == "Nho"


def test_reuses_ttns_cleaning():
    raw = to_ttns_raw(parse_export(_export(ROWS)), "Trái cây", PRODUCTS)
    df, stats = clean(raw, "Trái_cây.xls")
    assert stats["rows_clean"] == 3
    assert set(df["nhom_san_pham"]) == {"Trái cây"}
    assert dict(zip(df["tinh_cu"], df["tinh_moi"])) == {"Tiền Giang": "Đồng Tháp", "Sơn La": "Sơn La", "Hà Nội": "TP. Hà Nội"}
    assert df.loc[df["loai_gia"] == "Thu mua tại vườn", "cap_gia"].item() == "Thu mua"
    assert set(df["don_vi_tinh"]) == {"VNĐ/kg"}
    frames = build_tables(df)
    assert set(frames["dim_market"]["vung"]) == {"Đồng bằng sông Cửu Long", "Trung du và miền núi phía Bắc",
                                                 "Đồng bằng sông Hồng"}
    assert len(frames["fact_price_monthly"]) == 3
