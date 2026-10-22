"""Dựng sẵn lớp ngữ nghĩa OLAP + 2 dashboard trong Superset qua REST API (chạy lại bao nhiêu lần cũng được).

    docker compose exec superset python /app/bootstrap.py

Kiến trúc: ClickHouse là máy chủ OLAP (ROLAP: lược đồ hình sao + 3 khối con tính sẵn), Superset là công cụ OLAP phía
người dùng. Mỗi biểu đồ được gắn vào KHỐI CON NHỎ NHẤT trả lời được nó (ghi rõ trong tên biểu đồ); biểu đồ cần mức
chi tiết hơn mọi khối con thì đọc khung nhìn nguyên tử v_price_monthly.

Thao tác OLAP trên dashboard:
    cuộn lên / khoan xuống : đổi mức thời gian của biểu đồ, hoặc chuột phải > Drill by (đổi chiều phân tích)
    cắt lát / cắt khối     : bộ lọc dashboard (1 giá trị = cắt lát, nhiều chiều cùng lúc = cắt khối), bấm cột để lọc chéo
    xoay trục              : bảng pivot (đổi hàng <-> cột trong Explore)
"""
from __future__ import annotations

import json
import os
import sys
import uuid

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
USER, PASSWORD = os.environ.get("SUPERSET_USER", "admin"), os.environ.get("SUPERSET_PASSWORD", "admin")
DB_NAME = "Kho giá nông sản (ClickHouse)"
DB_URI = os.environ.get("CLICKHOUSE_URI", "clickhousedb://default:mypassword@clickhouse:8123/ttns")
SCHEMA = "ttns"
BAN_LE = "Bán lẻ"

# ============================================================== lớp ngữ nghĩa: dataset, cột tính, độ đo
# Trung bình phải tính lại từ tổng / số lần (độ đo đại số) - không lấy trung bình của trung bình.
# Trung vị lưu dạng trạng thái quantileExactState trong khối con -> gộp bằng quantileExactMerge (độ đo toàn thể).
CUBE_METRICS = [
    ("gia_tb", "Giá TB (VNĐ/kg)", "sum(tong_gia) / nullIf(sum(so_bao_gia), 0)", ",.0f"),
    ("gia_trung_vi", "Giá trung vị (VNĐ/kg)", "quantileExactMerge(0.5)(gia_tv)", ",.0f"),
    ("so_bao_gia", "Số lần báo giá", "sum(so_bao_gia)", ",d"),
    ("gia_thap_nhat", "Giá thấp nhất", "min(gia_min)", ",.0f"),
    ("gia_cao_nhat", "Giá cao nhất", "max(gia_max)", ",.0f"),
]


def _avg_year(y):
    return f"sumIf(tong_gia, year = {y}) / nullIf(sumIf(so_bao_gia, year = {y}), 0)"


VERBOSE = {
    "year": "Năm", "quarter": "Quý", "month": "Tháng", "month_no": "Tháng", "ky": "Kỳ",
    "nhom_san_pham": "Nhóm sản phẩm", "san_pham": "Sản phẩm", "mat_hang": "Mặt hàng",
    "tinh_cu": "Tỉnh (cũ)", "tinh_moi": "Tỉnh (mới)", "vung": "Vùng", "loai_gia": "Loại giá", "cap_gia": "Cấp giá",
    "don_vi_tinh": "Đơn vị tính", "cluster_name": "Cụm", "chuoi": "Chuỗi giá", "is_outlier": "Dị biệt (DBSCAN)",
    "bien_do_mua_vu": "Biên độ mùa vụ", "do_bien_dong": "Độ biến động", "xu_huong_nam": "Xu hướng/năm",
    "thang_dinh": "Tháng đỉnh giá", "last_price": "Giá gần nhất", "last_month": "Tháng gần nhất",
    "yhat": "Dự báo", "yhat_lo": "Cận dưới 80%", "yhat_hi": "Cận trên 80%", "pct_vs_last": "% so với giá gần nhất",
    "target_month": "Tháng dự báo",
}

DATASETS = {
    "base": {
        "table": "v_price_monthly",
        "description": "Khung nhìn nguyên tử: fact_price_monthly ⋈ 4 chiều (mức chi tiết nhất: tháng × mặt hàng × tỉnh × loại giá).",
        "dttm": ["month"],
        "calc": {},
        "metrics": [
            ("gia_tb", "Giá TB (VNĐ/kg)", "sum(sum_price) / nullIf(sum(n_valid), 0)", ",.0f"),
            ("so_bao_gia", "Số lần báo giá", "sum(n_valid)", ",d"),
            ("gia_thap_nhat", "Giá thấp nhất", "min(min_price)", ",.0f"),
            ("gia_cao_nhat", "Giá cao nhất", "max(max_price)", ",.0f"),
        ],
    },
    "cube_quy": {
        "table": "cube_quy_mathang_tinh_loaigia",
        "description": "Khối con (quý, mặt hàng, tỉnh cũ, loại giá) - chọn ở vòng 1 thuật toán tham lam.",
        "calc": {"ky": ("makeDate(year, (quarter - 1) * 3 + 1, 1)", True)},
        "metrics": CUBE_METRICS,
    },
    "cube_thang": {
        "table": "cube_thang_nhom_tinh_loaigia",
        "description": "Khối con (tháng, nhóm sản phẩm, tỉnh cũ, loại giá) - chọn ở vòng 2.",
        "calc": {"ky": ("makeDate(year, month, 1)", True)},
        "metrics": CUBE_METRICS,
    },
    "cube_nam": {
        "table": "cube_nam_mathang_tinh_loaigia",
        "description": "Khối con (năm, mặt hàng, tỉnh cũ, loại giá) - chọn ở vòng 3.",
        "calc": {"ky": ("makeDate(year, 1, 1)", True)},
        "metrics": CUBE_METRICS + [
            ("gia_tb_2024", "Giá TB 2024", _avg_year(2024), ",.0f"),
            ("gia_tb_2025", "Giá TB 2025", _avg_year(2025), ",.0f"),
            ("tang_2025_vs_2024", "Thay đổi 2025 so với 2024", f"({_avg_year(2025)}) / ({_avg_year(2024)}) - 1", "+.1%"),
        ],
    },
    "forecast": {
        "table": "ml_forecast",
        "description": "Dự báo giá tháng kế tiếp (Random Forest học độ thay đổi log giá) + khoảng 80% (conformal).",
        "dttm": ["target_month", "last_month"],
        "calc": {"chuoi": ("concat(mat_hang, ' | ', tinh_moi, ' | ', loai_gia)", False)},
        "metrics": [("pct_tb", "% thay đổi dự báo (TB)", "avg(pct_vs_last)", "+.1%")],
    },
    "cluster": {
        "table": "ml_cluster",
        "description": "Phân cụm K-Means (K=4) trên 6 đặc trưng hành vi + cờ dị biệt DBSCAN cho từng chuỗi giá.",
        "calc": {"chuoi": ("concat(mat_hang, ' | ', tinh_cu, ' | ', loai_gia)", False)},
        "metrics": [
            ("bien_do_mua_vu_tb", "Biên độ mùa vụ", "avg(bien_do_mua_vu)", ".0%"),
            ("do_bien_dong_tb", "Độ biến động tháng", "avg(do_bien_dong)", ".3f"),
            ("gia_trung_vi_tb", "Giá trung vị", "avg(gia_trung_vi)", ",.0f"),
            ("so_chuoi", "Số chuỗi", "count()", ",d"),
        ],
    },
    "metrics": {"table": "ml_metrics", "description": "Sai số mô hình trên năm kiểm tra.", "calc": {}, "metrics": []},
}


# ============================================================== biểu đồ
def flt(col, val, op="=="):
    return {"expressionType": "SIMPLE", "subject": col, "operator": op, "comparator": val, "clause": "WHERE"}


def fmt(**cols):
    """Định dạng số cho từng cột của bảng (chế độ raw không dùng d3format của độ đo)."""
    return {c: {"d3NumberFormat": f} for c, f in cols.items()}


RETAIL = [flt("cap_gia", BAN_LE)]
NO_TIME = lambda col: {"expressionType": "SIMPLE", "subject": col, "operator": "TEMPORAL_RANGE",  # noqa: E731
                       "comparator": "No filter", "clause": "WHERE"}

OLAP = "olap"
ML = "khai-pha"
CHARTS = [
    # key, dashboard, dataset, tên, viz, params
    ("bn_2025", OLAP, "cube_nam", "Giá bán lẻ TB 2025 · khối năm", "big_number_total",
     {"metric": "gia_tb_2025", "subheader": "VNĐ/kg, mọi mặt hàng, mọi tỉnh", "y_axis_format": ",.0f",
      "adhoc_filters": RETAIL}),
    ("bn_delta", OLAP, "cube_nam", "So với 2024 · khối năm", "big_number_total",
     {"metric": "tang_2025_vs_2024", "subheader": "giá bán lẻ TB 2025 / 2024 − 1", "y_axis_format": "+.1%",
      "adhoc_filters": RETAIL}),
    ("bn_n", OLAP, "cube_nam", "Số lần báo giá hợp lệ · khối năm", "big_number_total",
     {"metric": "so_bao_gia", "subheader": "đơn vị VNĐ/kg, đã loại giá bất thường", "y_axis_format": ",d",
      "adhoc_filters": []}),
    ("rollup_vung", OLAP, "cube_nam", "Cuộn lên: giá bán lẻ TB theo năm × vùng · khối năm", "echarts_timeseries_bar",
     {"x_axis": "year", "metrics": ["gia_tb"], "groupby": ["vung"], "adhoc_filters": RETAIL, "row_limit": 1000,
      "y_axis_format": ",.0f", "show_legend": True, "rich_tooltip": True, "x_axis_sort_asc": True,
      "orientation": "vertical", "show_value": False}),
    ("drill_thang", OLAP, "cube_thang", "Khoan xuống: giá bán lẻ TB theo tháng × nhóm · khối tháng", "echarts_timeseries_line",
     {"x_axis": "ky", "time_grain_sqla": "P1M", "metrics": ["gia_tb"], "groupby": ["nhom_san_pham"],
      "adhoc_filters": [NO_TIME("ky")] + RETAIL, "row_limit": 10000, "y_axis_format": ",.0f",
      "show_legend": True, "rich_tooltip": True, "markerEnabled": True, "markerSize": 4}),
    ("pivot_capgia", OLAP, "cube_quy", "Xoay trục: nhóm × cấp giá (thu mua → bán buôn → bán lẻ) · khối quý", "pivot_table_v2",
     {"groupbyRows": ["nhom_san_pham", "san_pham"], "groupbyColumns": ["cap_gia"], "metrics": ["gia_trung_vi"],
      "metricsLayout": "COLUMNS", "aggregateFunction": "Sum", "rowTotals": False, "colTotals": False,
      "valueFormat": ",.0f", "adhoc_filters": [flt("year", 2025)], "row_limit": 10000, "combineMetric": False}),
    ("heat_mua_vu", OLAP, "cube_thang", "Mùa vụ: giá bán lẻ theo tháng trong năm (chuẩn hoá theo nhóm) · khối tháng", "heatmap_v2",
     {"x_axis": "month", "groupby": "nhom_san_pham", "metric": "gia_tb", "normalize_across": "y",
      "linear_color_scheme": "superset_seq_1", "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
      "show_legend": True, "show_values": False, "y_axis_format": ",.0f", "adhoc_filters": RETAIL, "row_limit": 10000}),
    ("top_tang", OLAP, "cube_nam", "Mặt hàng tăng giá mạnh nhất 2025 so với 2024 (bán lẻ) · khối năm", "table",
     {"query_mode": "aggregate", "groupby": ["nhom_san_pham", "mat_hang"],
      "metrics": ["gia_tb_2024", "gia_tb_2025", "tang_2025_vs_2024"], "timeseries_limit_metric": "tang_2025_vs_2024",
      "order_desc": True, "row_limit": 15, "adhoc_filters": RETAIL + [flt("year", [2024, 2025], "IN")],
      "show_cell_bars": True, "include_search": True}),
    ("chi_tiet", OLAP, "base", "Chi tiết: tháng × mặt hàng × tỉnh × loại giá · bảng nguyên tử", "table",
     {"query_mode": "aggregate", "groupby": ["month", "nhom_san_pham", "mat_hang", "tinh_moi", "loai_gia"],
      "metrics": ["gia_tb", "so_bao_gia", "gia_thap_nhat", "gia_cao_nhat"], "timeseries_limit_metric": "so_bao_gia",
      "order_desc": True, "row_limit": 500, "adhoc_filters": [NO_TIME("month"), flt("don_vi_tinh", "VNĐ/kg")],
      "include_search": True, "time_grain_sqla": "P1M", "table_timestamp_format": "%m/%Y"}),
    # ---------------------------------------------------------------- khai phá
    ("ml_sai_so", ML, "metrics", "Sai số mô hình dự báo (năm kiểm tra 2025)", "table",
     {"query_mode": "raw", "all_columns": ["model", "rmse", "mae", "mape", "r2"], "row_limit": 20,
      "adhoc_filters": [], "column_config": fmt(rmse=",.0f", mae=",.0f", mape=".1%", r2=".3f")}),
    ("ml_du_bao", ML, "forecast", "Dự báo giá tháng tới kèm khoảng 80%", "table",
     {"query_mode": "raw", "all_columns": ["target_month", "nhom_san_pham", "mat_hang", "tinh_moi", "loai_gia",
                                           "last_price", "yhat", "yhat_lo", "yhat_hi", "pct_vs_last"],
      "order_by_cols": [json.dumps(["pct_vs_last", False])], "row_limit": 500, "include_search": True,
      "adhoc_filters": [NO_TIME("target_month")], "table_timestamp_format": "%m/%Y",
      "column_config": fmt(last_price=",.0f", yhat=",.0f", yhat_lo=",.0f", yhat_hi=",.0f", pct_vs_last="+.1%")}),
    ("ml_du_bao_nhom", ML, "forecast", "Thay đổi giá dự báo TB theo nhóm × cấp giá", "echarts_timeseries_bar",
     {"x_axis": "nhom_san_pham", "metrics": ["pct_tb"], "groupby": ["cap_gia"], "row_limit": 1000,
      "y_axis_format": "+.1%", "show_legend": True, "orientation": "vertical", "adhoc_filters": []}),
    ("ml_bubble", ML, "cluster", "Phân cụm K-Means: biên độ mùa vụ × độ biến động", "bubble_v2",
     {"entity": "chuoi", "series": "cluster_name", "x": "bien_do_mua_vu_tb", "y": "do_bien_dong_tb",
      "size": "gia_trung_vi_tb", "max_bubble_size": "15", "row_limit": 1000, "x_axis_format": ".0%",
      "y_axis_format": ".3f", "show_legend": True, "adhoc_filters": [],
      "x_axis_label": "Biên độ mùa vụ", "y_axis_label": "Độ biến động tháng"}),
    ("ml_so_chuoi", ML, "cluster", "Số chuỗi giá mỗi cụm", "pie",
     {"groupby": ["cluster_name"], "metric": "so_chuoi", "show_labels": True, "label_type": "value",
      "show_legend": True, "legendOrientation": "bottom", "legendType": "plain", "donut": True, "adhoc_filters": [], "row_limit": 100}),
    ("ml_di_biet", ML, "cluster", "Chuỗi giá dị biệt (DBSCAN gán nhãn nhiễu −1)", "table",
     {"query_mode": "raw", "all_columns": ["mat_hang", "tinh_cu", "loai_gia", "cluster_name", "bien_do_mua_vu",
                                           "do_bien_dong", "xu_huong_nam", "thang_dinh", "so_thang"],
      "adhoc_filters": [flt("is_outlier", 1)], "row_limit": 100,
      "column_config": fmt(bien_do_mua_vu=".0%", do_bien_dong=".3f", xu_huong_nam="+.1%")}),
]

DASHBOARDS = {
    OLAP: {
        "title": "Kho giá nông sản — OLAP",
        "intro": (
            "**Máy chủ OLAP:** ClickHouse (ROLAP, lược đồ hình sao 4 chiều). Mỗi biểu đồ đọc **khối con nhỏ nhất** trả lời "
            "được nó (ghi sau dấu ·), không đọc 40.575 dòng gốc.  \n"
            "**Cuộn lên / khoan xuống:** trong Explore đổi *Time grain* (tháng ↔ quý ↔ năm) hoặc chuột phải › *Drill by* "
            "(nhóm → sản phẩm → mặt hàng, vùng → tỉnh). **Cắt lát / cắt khối:** bộ lọc bên trái, bấm vào cột để lọc chéo. "
            "**Xoay trục:** bảng pivot."),
        "rows": [[("bn_2025", 4), ("bn_delta", 4), ("bn_n", 4)],
                 [("rollup_vung", 5), ("drill_thang", 7)],
                 [("pivot_capgia", 6), ("heat_mua_vu", 6)],
                 [("top_tang", 12)],
                 [("chi_tiet", 12)]],
        "filters": [("year", "Năm"), ("vung", "Vùng"), ("tinh_moi", "Tỉnh (mới)"), ("nhom_san_pham", "Nhóm sản phẩm"),
                    ("mat_hang", "Mặt hàng"), ("cap_gia", "Cấp giá")],
    },
    ML: {
        "title": "Kho giá nông sản — Khai phá",
        "intro": (
            "Kết quả khai phá được ghi ngược vào kho (`ttns.ml_*`) bởi `ml/publish_ml.py`.  \n"
            "**Dự báo:** Random Forest học độ thay đổi log(giá) so với tháng trước; so với mô hình cơ sở *giữ nguyên giá*. "
            "Khoảng 80% theo phương pháp conformal.  \n"
            "**Phân cụm:** K-Means (K = 4, chọn bằng Elbow + Silhouette) trên 6 đặc trưng hành vi đã chuẩn hoá. "
            "**Dị biệt:** DBSCAN, eps lấy tại điểm gãy của đồ thị k-distance."),
        "rows": [[("ml_sai_so", 5), ("ml_du_bao_nhom", 7)],
                 [("ml_du_bao", 12)],
                 [("ml_bubble", 8), ("ml_so_chuoi", 4)],
                 [("ml_di_biet", 12)]],
        "filters": [("nhom_san_pham", "Nhóm sản phẩm"), ("vung", "Vùng"), ("cap_gia", "Cấp giá"),
                    ("cluster_name", "Cụm")],
    },
}
FILTER_DATASET = {OLAP: "base", ML: "cluster"}


# ============================================================== REST API
class Client:
    def __init__(self):
        self.s = requests.Session()
        r = self.s.post(f"{BASE}/api/v1/security/login",
                        json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True})
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": BASE})

    def call(self, method, path, **kw):
        r = self.s.request(method, f"{BASE}/api/v1/{path}", **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:800]}")
        return r.json() if r.text else {}

    def list(self, resource, cols):
        q = f"(columns:!({','.join(cols)}),page_size:100,page:%d)"
        out, page = [], 0
        while True:
            res = self.call("GET", f"{resource}/", params={"q": q % page})["result"]
            out += res
            if len(res) < 100:
                return out
            page += 1


COL_KEYS = {"id", "column_name", "type", "verbose_name", "description", "expression", "filterable", "groupby",
            "is_active", "is_dttm", "python_date_format", "extra", "advanced_data_type", "uuid"}
MET_KEYS = {"id", "metric_name", "expression", "verbose_name", "d3format", "description", "metric_type",
            "warning_text", "extra", "currency", "uuid"}


def ensure_database(c: Client) -> int:
    payload = {"database_name": DB_NAME, "sqlalchemy_uri": DB_URI, "expose_in_sqllab": True,
               "allow_run_async": False, "allow_dml": False}
    for d in c.list("database", ["id", "database_name"]):
        if d["database_name"] == DB_NAME:
            c.call("PUT", f"database/{d['id']}", json=payload)
            return d["id"]
    return c.call("POST", "database/", json=payload)["id"]


def ensure_dataset(c: Client, db_id: int, spec: dict) -> int:
    existing = {(d["schema"], d["table_name"]): d["id"]
                for d in c.list("dataset", ["id", "schema", "table_name"])}
    ds_id = existing.get((SCHEMA, spec["table"]))
    if ds_id is None:
        ds_id = c.call("POST", "dataset/", json={"database": db_id, "schema": SCHEMA, "table_name": spec["table"]})["id"]
    else:   # đồng bộ cột với bảng thật (bảng có thể đã thêm cột)
        c.call("PUT", f"dataset/{ds_id}/refresh")
    cur = c.call("GET", f"dataset/{ds_id}")["result"]

    cols = []
    for col in cur["columns"]:
        name = col["column_name"]
        if name in spec["calc"] or (col.get("expression") and name not in spec["calc"]):
            continue                                           # cột tính: tạo lại bên dưới
        col = {k: v for k, v in col.items() if k in COL_KEYS and v is not None}
        col["verbose_name"] = VERBOSE.get(name, col.get("verbose_name"))
        col["is_dttm"] = name in spec.get("dttm", []) or bool(col.get("is_dttm"))
        cols.append(col)
    old_calc = {col["column_name"]: col["id"] for col in cur["columns"] if col.get("expression")}
    for name, (expr, is_dttm) in spec["calc"].items():
        col = {"column_name": name, "expression": expr, "is_dttm": is_dttm, "verbose_name": VERBOSE.get(name),
               "type": "DATE" if is_dttm else "STRING", "filterable": True, "groupby": True}
        if name in old_calc:
            col["id"] = old_calc[name]
        cols.append(col)

    old_met = {m["metric_name"]: m for m in cur["metrics"]}
    mets = [{k: v for k, v in m.items() if k in MET_KEYS and v is not None}
            for name, m in old_met.items() if name not in {x[0] for x in spec["metrics"]}]
    for name, label, expr, fmt in spec["metrics"]:
        m = {"metric_name": name, "verbose_name": label, "expression": expr, "d3format": fmt}
        if name in old_met:
            m["id"] = old_met[name]["id"]
        mets.append(m)

    main_dttm = (spec.get("dttm") or [n for n, (_, d) in spec["calc"].items() if d] or [None])[0]
    body = {"columns": cols, "metrics": mets, "description": spec["description"]}
    if main_dttm:
        body["main_dttm_col"] = main_dttm
    c.call("PUT", f"dataset/{ds_id}", json=body)
    return ds_id


def query_check(c: Client, ds_id: int, viz: str, p: dict) -> int:
    """Chạy thử truy vấn tương ứng biểu đồ qua /chart/data (bắt lỗi SQL ngay khi dựng). Trả về số dòng."""
    filters = [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
               for f in p.get("adhoc_filters", []) if f["operator"] != "TEMPORAL_RANGE"]
    if p.get("query_mode") == "raw":
        q = {"columns": p["all_columns"], "metrics": []}
    elif viz == "bubble_v2":
        q = {"columns": [p["entity"], p["series"]], "metrics": [p["x"], p["y"], p["size"]]}
    elif viz == "heatmap_v2":
        q = {"columns": [p["x_axis"], p["groupby"]], "metrics": [p["metric"]]}
    elif viz == "pivot_table_v2":
        q = {"columns": p["groupbyRows"] + p["groupbyColumns"], "metrics": p["metrics"]}
    else:
        cols = ([p["x_axis"]] if "x_axis" in p else []) + p.get("groupby", [])
        q = {"columns": cols, "metrics": p.get("metrics") or [p["metric"]]}
    q.update(filters=filters, row_limit=p.get("row_limit", 1000), extras={"where": "", "having": ""})
    res = c.call("POST", "chart/data", json={"datasource": {"id": ds_id, "type": "table"}, "force": True,
                                              "queries": [q], "result_format": "json", "result_type": "full"})
    r = res["result"][0]
    if r.get("error"):
        raise RuntimeError(r["error"])
    return r["rowcount"]


def layout(title: str, intro: str, rows, chart_ids: dict, names: dict) -> dict:
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
           "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}}}

    def row(children):
        rid = f"ROW-{uuid.uuid4().hex[:10]}"
        pos["GRID_ID"]["children"].append(rid)
        pos[rid] = {"type": "ROW", "id": rid, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for kind, meta in children:
            cid = f"{kind}-{uuid.uuid4().hex[:10]}"
            pos[rid]["children"].append(cid)
            pos[cid] = {"type": kind, "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid], "meta": meta}

    row([("MARKDOWN", {"width": 12, "height": 15, "code": intro})])
    for r in rows:
        row([("CHART", {"chartId": chart_ids[k], "width": w, "height": 32 if k.startswith("bn_") else 60,
                        "sliceName": names[k]}) for k, w in r])
    return pos


def native_filters(ds_id: int, filters) -> list[dict]:
    return [{"id": f"NATIVE_FILTER-{col}", "name": label, "filterType": "filter_select", "type": "NATIVE_FILTER",
             "targets": [{"datasetId": ds_id, "column": {"name": col}}],
             "controlValues": {"multiSelect": True, "enableEmptyFilter": False, "defaultToFirstItem": False,
                               "searchAllOptions": False, "inverseSelection": False},
             "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
             "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "description": ""}
            for col, label in filters]


def main():
    c = Client()
    db_id = ensure_database(c)
    ds = {k: ensure_dataset(c, db_id, spec) for k, spec in DATASETS.items()}
    print(f"Database id={db_id}; datasets: " + ", ".join(f"{k}={v}" for k, v in ds.items()))

    # xoá dashboard + biểu đồ do script tạo lần trước để chạy lại không bị nhân đôi
    slugs = {f"nong-san-{k}" for k in DASHBOARDS}
    for d in c.list("dashboard", ["id", "slug"]):
        if d["slug"] in slugs:
            c.call("DELETE", f"dashboard/{d['id']}")
    names = {key: name for key, _, _, name, _, _ in CHARTS}
    for ch in c.list("chart", ["id", "slice_name"]):
        if ch["slice_name"] in names.values():
            c.call("DELETE", f"chart/{ch['id']}")

    chart_ids, failed = {}, []
    for key, dash, dskey, name, viz, params in CHARTS:
        p = {"viz_type": viz, "datasource": f"{ds[dskey]}__table", "color_scheme": "supersetColors", **params}
        try:
            n = query_check(c, ds[dskey], viz, p)
        except RuntimeError as e:
            failed.append(name)
            print(f"  LỖI {name}: {e}")
            continue
        chart_ids[key] = c.call("POST", "chart/", json={
            "slice_name": name, "viz_type": viz, "datasource_id": ds[dskey], "datasource_type": "table",
            "params": json.dumps(p, ensure_ascii=False)})["id"]
        print(f"  biểu đồ {chart_ids[key]:>3}  {n:>5} dòng  {name}")
    if failed:
        sys.exit(f"{len(failed)} biểu đồ lỗi truy vấn")

    for key, spec in DASHBOARDS.items():
        meta = {"native_filter_configuration": native_filters(ds[FILTER_DATASET[key]], spec["filters"]),
                "cross_filters_enabled": True, "color_scheme": "supersetColors", "refresh_frequency": 0,
                "timed_refresh_immune_slices": [], "expanded_slices": {}, "label_colors": {}}
        dash_id = c.call("POST", "dashboard/", json={
            "dashboard_title": spec["title"], "slug": f"nong-san-{key}", "published": True,
            "position_json": json.dumps(layout(spec["title"], spec["intro"], spec["rows"], chart_ids, names),
                                        ensure_ascii=False),
            "json_metadata": json.dumps(meta, ensure_ascii=False)})["id"]
        for k, w in (x for r in spec["rows"] for x in r):
            c.call("PUT", f"chart/{chart_ids[k]}", json={"dashboards": [dash_id]})
        print(f"Dashboard: {BASE}/superset/dashboard/nong-san-{key}/")


if __name__ == "__main__":
    main()
