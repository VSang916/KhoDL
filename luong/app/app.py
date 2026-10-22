"""
Ứng dụng khai phá & phân tích giá nông sản (CLO2.TC2) — chạy:  streamlit run app/app.py
Đọc trực tiếp kho ClickHouse (localhost:8123): khối con OLAP, bảng sự kiện và kết quả khai phá (ml_*).

Tab OLAP xử lý truy vấn đúng quy trình Bài 5 (slide 40): xác định khối con trả lời được -> chọn khối đã vật chất hoá
NHỎ NHẤT -> độ đo phải cuộn tiếp được (tổng, đếm, min, max, trạng thái trung vị) -> không có thì quay về dữ liệu nguyên tử.
"""
from __future__ import annotations

import io
import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import requests
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
from pipelines.ttns import olap  # noqa: E402  (định nghĩa khối con dùng chung với DAG)

CH_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CH_AUTH = ("default", os.getenv("CLICKHOUSE_PASSWORD", "mypassword"))
CAP_COLOR = alt.Scale(domain=["Bán lẻ", "Thu mua", "Bán buôn"], range=["#2a78d6", "#eb6834", "#1baf7a"])

st.set_page_config(page_title="Kho dữ liệu giá nông sản", layout="wide")


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    r = requests.post(CH_URL, data=(sql.strip() + "\nFORMAT CSVWithNames").encode("utf-8"), auth=CH_AUTH, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(r.text[:400])
    return pd.read_csv(io.StringIO(r.text)) if r.text.strip() else pd.DataFrame()


def sql_list(values) -> str:
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


try:
    q("SELECT 1")
except Exception as exc:  # noqa: BLE001
    st.error(f"Không kết nối được ClickHouse ở {CH_URL}. Hãy chạy `docker compose up -d` trong thư mục luong/.\n\n{exc}")
    st.stop()

st.title("Kho dữ liệu giá rau quả Việt Nam")
st.caption("Nguồn: thitruongnongsan.gov.vn (giá rau quả trong nước, cộng tác viên địa phương) · luồng Airflow → MinIO → "
           "ClickHouse · đơn vị VNĐ/kg")
tab_overview, tab_olap, tab_fc, tab_cl, tab_about = st.tabs(
    ["Tổng quan", "OLAP", "Dự báo giá", "Phân cụm & dị biệt", "Về kho dữ liệu"])

# =====================================================================================================================
with tab_overview:
    k = q("""SELECT count() AS dong, uniqExact(san_pham) AS sp, uniqExact(mat_hang) AS mh, uniqExact(tinh_cu) AS tinh,
                    min(month) AS tu, max(month) AS den, sum(n_obs) AS bao_gia FROM ttns.v_price_monthly""").iloc[0]
    c = st.columns(5)
    c[0].metric("Lần báo giá", f"{k['bao_gia']:,}")
    c[1].metric("Bản ghi tháng", f"{k['dong']:,}")
    c[2].metric("Sản phẩm / mặt hàng", f"{k['sp']} / {k['mh']}")
    c[3].metric("Tỉnh", k["tinh"])
    c[4].metric("Thời gian", f"{k['tu'][:7]} → {k['den'][:7]}")

    trend = q("""SELECT toDate(concat(toString(year), '-', leftPad(toString(month), 2, '0'), '-01')) AS thang, cap_gia,
                        round(sum(tong_gia) / sum(so_bao_gia)) AS gia_tb, sum(so_bao_gia) AS n_bao_gia
                 FROM ttns.cube_thang_nhom_tinh_loaigia GROUP BY thang, cap_gia ORDER BY thang""")
    trend["thang"] = pd.to_datetime(trend["thang"])
    st.subheader("Giá trung bình theo tháng và cấp giá")
    st.altair_chart(alt.Chart(trend).mark_line(strokeWidth=2).encode(
        x=alt.X("thang:T", title=None), y=alt.Y("gia_tb:Q", title="VNĐ/kg"),
        color=alt.Color("cap_gia:N", scale=CAP_COLOR, title="Cấp giá"),
        tooltip=[alt.Tooltip("thang:T", format="%m/%Y"), "cap_gia", alt.Tooltip("gia_tb:Q", format=",.0f"),
                 alt.Tooltip("n_bao_gia:Q", format=",")]).properties(height=320), width="stretch")
    cdc = q("""SELECT run_date, change_kind, count() AS so_khoa FROM ttns.cdc_log GROUP BY run_date, change_kind
               ORDER BY run_date DESC, so_khoa DESC LIMIT 20""")
    with st.expander("Thay đổi dữ liệu nguồn phát hiện bằng CDC (ttns.cdc_log)"):
        st.dataframe(cdc, width="stretch", hide_index=True)

# =====================================================================================================================
DIMS = {  # chiều: (tên, các mức mịn -> thô, nhãn mức)
    "thoi_gian": ("Thời gian", ["thang", "quy", "nam", None], ["Tháng", "Quý", "Năm", "(tất cả)"]),
    "san_pham": ("Sản phẩm", ["mat_hang", "san_pham", "nhom", None], ["Mặt hàng", "Sản phẩm", "Nhóm", "(tất cả)"]),
    "dia_diem": ("Địa điểm", ["tinh_cu", "tinh_moi", "vung", None], ["Tỉnh cũ", "Tỉnh mới", "Vùng", "(tất cả)"]),
    "loai_gia": ("Loại giá", ["loai_gia", "cap_gia", None], ["Loại giá", "Cấp giá", "(tất cả)"]),
}
FILTERS = {  # thuộc tính lọc -> (chiều, chỉ số mức của thuộc tính, nhãn)
    "year": ("thoi_gian", 2, "Năm"), "nhom_san_pham": ("san_pham", 2, "Nhóm"), "san_pham": ("san_pham", 1, "Sản phẩm"),
    "vung": ("dia_diem", 2, "Vùng"), "tinh_moi": ("dia_diem", 1, "Tỉnh mới"), "cap_gia": ("loai_gia", 1, "Cấp giá"),
}
MEASURES = {"gia_tb": "Giá trung bình", "gia_trung_vi": "Giá trung vị", "n_bao_gia": "Số lần báo giá",
            "gia_thap_nhat": "Giá thấp nhất", "gia_cao_nhat": "Giá cao nhất"}
HIDE = {"product_id", "market_id"}


def choose_source(required: dict[str, int]):
    """Khối con đã vật chất hoá nhỏ nhất trả lời được (mọi chiều của khối mịn bằng/hơn mức yêu cầu), hoặc None."""
    sizes = q("SELECT name, total_rows FROM system.tables WHERE database = 'ttns' AND name LIKE 'cube_%'")
    sizes = dict(zip(sizes["name"], sizes["total_rows"]))
    best = None
    for name, spec in olap.CUBOIDS.items():
        ok = all(DIMS[d][1].index(spec["levels"][d]) <= required[d] for d in DIMS)
        if ok and name in sizes and (best is None or sizes[name] < sizes[best]):
            best = name
    return best, sizes


with tab_olap:
    st.markdown("Chọn **mức** của từng chiều (kéo sang phải = *cuộn lên*, sang trái = *khoan xuống*), **lọc** để *cắt "
                "lát* (1 chiều) hoặc *cắt khối* (nhiều chiều), và chọn hàng / cột để *xoay trục*.")
    lv_cols = st.columns(4)
    chosen = {}
    for col, (dim, (label, levels, names)) in zip(lv_cols, DIMS.items()):
        default = {"thoi_gian": "Năm", "san_pham": "Nhóm", "dia_diem": "(tất cả)", "loai_gia": "Cấp giá"}[dim]
        pick = col.select_slider(label, options=names, value=default, key=f"lv_{dim}")
        chosen[dim] = names.index(pick)

    opts = q("""SELECT DISTINCT year, nhom_san_pham, san_pham, vung, tinh_moi, cap_gia
                FROM ttns.cube_quy_mathang_tinh_loaigia""")
    f_cols = st.columns(6)
    filters = {}
    for col, (attr, (_, _, label)) in zip(f_cols, FILTERS.items()):
        vals = col.multiselect(label, sorted(opts[attr].dropna().unique().tolist()), key=f"f_{attr}")
        if vals:
            filters[attr] = vals

    required = dict(chosen)
    for attr in filters:
        dim, idx, _ = FILTERS[attr]
        required[dim] = min(required[dim], idx)
    source, sizes = choose_source(required)

    group_cols = []
    for dim, idx in chosen.items():
        level = DIMS[dim][1][idx]
        group_cols += [c for c in olap.LEVEL_COLS[dim][level] if c not in HIDE]

    if source:
        where = " AND ".join(f"{a} IN ({sql_list(v)})" for a, v in filters.items())
        sql = (f"SELECT {', '.join(group_cols) + ', ' if group_cols else ''}sum(so_bao_gia) AS n_bao_gia, "
               "round(sum(tong_gia) / sum(so_bao_gia)) AS gia_tb, quantileExactMerge(0.5)(gia_tv) AS gia_trung_vi, "
               "min(gia_min) AS gia_thap_nhat, max(gia_max) AS gia_cao_nhat "
               f"FROM ttns.{source}" + (f" WHERE {where}" if where else "") +
               (f" GROUP BY {', '.join(group_cols)} ORDER BY {', '.join(group_cols)}" if group_cols else ""))
        st.success(f"Trả lời bằng khối con **{source}** ({sizes[source]:,} dòng) thay vì đọc toàn bộ dữ liệu nguyên tử.")
    else:
        expr = olap.COL_EXPR
        where = " AND ".join(f"{expr['year'] if a == 'year' else expr[a]} IN ({sql_list(v)})" for a, v in filters.items())
        sel = ", ".join(f"{expr[c]} AS {c}" for c in group_cols)
        sql = (f"SELECT {sel + ', ' if sel else ''}count() AS n_bao_gia, round(avg(c.gia)) AS gia_tb, "
               "quantileExact(0.5)(c.gia) AS gia_trung_vi, min(c.gia) AS gia_thap_nhat, max(c.gia) AS gia_cao_nhat "
               f"{olap.BASE_FROM}" + (f" AND {where}" if where else "") +
               (f" GROUP BY {', '.join(group_cols)} ORDER BY {', '.join(group_cols)}" if group_cols else ""))
        st.warning("Không khối con nào đủ mịn cho tổ hợp này → truy vấn dữ liệu nguyên tử (ttns.price_clean).")
    with st.expander("Câu SQL đã chạy"):
        st.code(sql, language="sql")

    res = q(sql)
    st.caption(f"{len(res):,} ô")
    m_cols = st.columns([2, 2, 2])
    measure = m_cols[0].selectbox("Độ đo", list(MEASURES), format_func=MEASURES.get)
    dims_avail = [c for c in group_cols]
    row_dims = m_cols[1].multiselect("Hàng (xoay trục)", dims_avail, default=dims_avail[:-1] if len(dims_avail) > 1 else dims_avail)
    col_dim = m_cols[2].selectbox("Cột", ["(không)"] + [c for c in dims_avail if c not in row_dims])
    if col_dim != "(không)" and row_dims:
        pivot = res.pivot_table(index=row_dims, columns=col_dim, values=measure, aggfunc="first")
        st.dataframe(pivot.style.format("{:,.0f}"), width="stretch")
    else:
        st.dataframe(res, width="stretch", hide_index=True)

    if "month" in group_cols or "quarter" in group_cols or ("year" in group_cols and res["year"].nunique() > 1):
        tcol = "month" if "month" in group_cols else ("quarter" if "quarter" in group_cols else None)
        plot = res.copy()
        plot["ky"] = plot["year"].astype(str) + ((("-" + plot[tcol].astype(str).str.zfill(2)) if tcol == "month"
                                                 else ("-Q" + plot[tcol].astype(str))) if tcol else "")
        others = [c for c in group_cols if c not in ("year", "quarter", "month")]
        plot["chuoi"] = plot[others].astype(str).agg(" · ".join, axis=1) if others else MEASURES[measure]
        if plot["chuoi"].nunique() <= 8:
            st.altair_chart(alt.Chart(plot).mark_line(point=True).encode(
                x=alt.X("ky:O", title=None), y=alt.Y(f"{measure}:Q", title=MEASURES[measure]),
                color=alt.Color("chuoi:N", title=None), tooltip=["ky", "chuoi", alt.Tooltip(f"{measure}:Q", format=",.0f")]
            ).properties(height=320), width="stretch")
        else:
            st.caption("Quá 8 chuỗi để vẽ — lọc bớt để xem biểu đồ.")

# =====================================================================================================================
with tab_fc:
    met = q("SELECT model, test_year, rmse, mae, r2, mape FROM ttns.ml_metrics ORDER BY rmse")
    fc = q("SELECT * FROM ttns.ml_forecast")
    if fc.empty:
        st.info("Chưa có dự báo — chạy `python ml/publish_ml.py`.")
    else:
        st.markdown(f"Dự báo giá **{fc['target_month'].iloc[0][:7]}** bằng *Random Forest học độ thay đổi so với lần báo "
                    "giá trước*, kèm **khoảng 80%** tính từ sai số thật trên năm kiểm tra.")
        c1, c2, c3, c4 = st.columns(4)
        sp = c1.selectbox("Sản phẩm", sorted(fc["san_pham"].unique()))
        f1 = fc[fc["san_pham"] == sp]
        mh = c2.selectbox("Mặt hàng", sorted(f1["mat_hang"].unique()))
        f2 = f1[f1["mat_hang"] == mh]
        tinh = c3.selectbox("Tỉnh", sorted(f2["tinh_cu"].unique()))
        f3 = f2[f2["tinh_cu"] == tinh]
        lg = c4.selectbox("Loại giá", sorted(f3["loai_gia"].unique()))
        row = f3[f3["loai_gia"] == lg].iloc[0]

        hist = q(f"""SELECT month, median_price FROM ttns.v_price_monthly
                     WHERE san_pham = {sql_list([sp])} AND mat_hang = {sql_list([mh])} AND tinh_cu = {sql_list([tinh])}
                       AND loai_gia = {sql_list([lg])} AND don_vi_tinh = 'VNĐ/kg' ORDER BY month""")
        hist["month"] = pd.to_datetime(hist["month"])
        m = st.columns(3)
        m[0].metric(f"Giá gần nhất ({row['last_month'][:7]})", f"{row['last_price']:,.0f} đ/kg")
        m[1].metric(f"Dự báo {row['target_month'][:7]}", f"{row['yhat']:,.0f} đ/kg", f"{row['pct_vs_last']:+.1%}")
        m[2].metric("Khoảng 80%", f"{row['yhat_lo']:,.0f} – {row['yhat_hi']:,.0f}")
        pt = pd.DataFrame({"month": [pd.to_datetime(row["target_month"])], "yhat": [row["yhat"]],
                           "lo": [row["yhat_lo"]], "hi": [row["yhat_hi"]]})
        line = alt.Chart(hist).mark_line(point=True, color="#2a78d6").encode(
            x=alt.X("month:T", title=None), y=alt.Y("median_price:Q", title="VNĐ/kg"),
            tooltip=[alt.Tooltip("month:T", format="%m/%Y"), alt.Tooltip("median_price:Q", format=",.0f")])
        band = alt.Chart(pt).mark_errorbar(color="#eb6834", thickness=2, ticks=True).encode(
            x="month:T", y=alt.Y("lo:Q", title="VNĐ/kg"), y2="hi:Q")
        dot = alt.Chart(pt).mark_point(color="#eb6834", size=120, filled=True).encode(
            x="month:T", y="yhat:Q", tooltip=[alt.Tooltip("yhat:Q", format=",.0f", title="dự báo"),
                                              alt.Tooltip("lo:Q", format=",.0f"), alt.Tooltip("hi:Q", format=",.0f")])
        st.altair_chart((line + band + dot).properties(height=320), width="stretch")

        st.subheader("Dự kiến tăng / giảm mạnh nhất tháng tới")
        show = fc[["san_pham", "mat_hang", "tinh_cu", "loai_gia", "last_price", "yhat", "yhat_lo", "yhat_hi", "pct_vs_last"]]
        a, b = st.columns(2)
        fmt = {"last_price": "{:,.0f}", "yhat": "{:,.0f}", "yhat_lo": "{:,.0f}", "yhat_hi": "{:,.0f}", "pct_vs_last": "{:+.1%}"}
        a.markdown("**Tăng**")
        a.dataframe(show.nlargest(10, "pct_vs_last").style.format(fmt), hide_index=True, width="stretch")
        b.markdown("**Giảm**")
        b.dataframe(show.nsmallest(10, "pct_vs_last").style.format(fmt), hide_index=True, width="stretch")
        with st.expander("Độ chính xác trên năm kiểm tra (so với mô hình cơ sở)"):
            st.dataframe(met.style.format({"rmse": "{:,.0f}", "mae": "{:,.0f}", "r2": "{:.3f}", "mape": "{:.1%}"}),
                         hide_index=True, width="stretch")
            st.caption("Mô hình giảm sai số lớn (RMSE) so với 'giữ nguyên giá', nhưng sai số điển hình (MAE) tương đương — "
                       "giá rau quả theo tháng có quán tính lớn. Chi tiết: ml/DeThucHanh01_GiaNongSan.ipynb.")

# =====================================================================================================================
with tab_cl:
    cl = q("SELECT * FROM ttns.ml_cluster")
    if cl.empty:
        st.info("Chưa có kết quả phân cụm — chạy `python ml/publish_ml.py`.")
    else:
        st.markdown("Mỗi điểm là một **chuỗi giá** (mặt hàng × tỉnh × loại giá, ≥ 24 tháng), phân cụm K-Means theo "
                    "**hành vi giá** (biên độ mùa vụ, tháng đỉnh, độ biến động, xu hướng, tỷ lệ tháng đứng giá) — không dùng "
                    "mức giá. DBSCAN đánh dấu các chuỗi **dị biệt** (không thuộc vùng dày đặc nào).")
        summ = cl.groupby("cluster_name").agg(so_chuoi=("mat_hang", "size"), bien_do_mua_vu=("bien_do_mua_vu", "mean"),
                                              do_bien_dong=("do_bien_dong", "mean"), xu_huong_nam=("xu_huong_nam", "mean"),
                                              ty_le_dung_gia=("ty_le_dung_gia", "mean"),
                                              san_pham_tieu_bieu=("san_pham", lambda s: ", ".join(s.value_counts().index[:5])))
        st.dataframe(summ.style.format({"bien_do_mua_vu": "{:.0%}", "do_bien_dong": "{:.3f}", "xu_huong_nam": "{:+.1%}",
                                        "ty_le_dung_gia": "{:.0%}"}), width="stretch")
        cl["loai"] = cl["is_outlier"].map({1: "dị biệt (DBSCAN)", 0: "bình thường"})
        st.altair_chart(alt.Chart(cl).mark_point(filled=True, size=70).encode(
            x=alt.X("bien_do_mua_vu:Q", title="biên độ mùa vụ", axis=alt.Axis(format="%")),
            y=alt.Y("do_bien_dong:Q", title="độ biến động tháng"),
            color=alt.Color("cluster_name:N", title="Cụm"), shape=alt.Shape("loai:N", title=None),
            tooltip=["san_pham", "mat_hang", "tinh_cu", "loai_gia", "cluster_name",
                     alt.Tooltip("bien_do_mua_vu:Q", format=".0%"), alt.Tooltip("xu_huong_nam:Q", format="+.1%")]
        ).properties(height=380), width="stretch")
        pick = st.selectbox("Xem các chuỗi của cụm", sorted(cl["cluster_name"].unique()))
        st.dataframe(cl[cl["cluster_name"] == pick][["san_pham", "mat_hang", "tinh_cu", "loai_gia", "thang_dinh",
                                                    "bien_do_mua_vu", "do_bien_dong", "xu_huong_nam"]],
                     hide_index=True, width="stretch")
        st.subheader("Chuỗi giá dị biệt")
        st.dataframe(cl[cl["is_outlier"] == 1][["san_pham", "mat_hang", "tinh_cu", "loai_gia", "bien_do_mua_vu",
                                                "do_bien_dong", "xu_huong_nam", "ty_le_dung_gia"]],
                     hide_index=True, width="stretch")

# =====================================================================================================================
with tab_about:
    img = ROOT / "olap" / "figures" / "01_luoc_do_hinh_sao.png"
    if img.exists():
        st.image(str(img), caption="Lược đồ hình sao")
    st.markdown("**Khối con vật chất hoá** (chọn bằng thuật toán tham lam — xem olap/OLAP_GiaNongSan.ipynb):")
    cubes = q("SELECT name AS khoi_con, total_rows AS so_dong, comment AS ly_do FROM system.tables "
              "WHERE database = 'ttns' AND name LIKE 'cube_%' ORDER BY total_rows DESC")
    st.dataframe(cubes, hide_index=True, width="stretch")
    st.markdown("**Bảng trong kho `ttns`:**")
    st.dataframe(q("SELECT name AS bang, engine, total_rows AS so_dong FROM system.tables "
                   "WHERE database = 'ttns' ORDER BY name"), hide_index=True, width="stretch")
