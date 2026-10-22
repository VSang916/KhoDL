"""
Huấn luyện mô hình cuối cùng và ĐƯA KẾT QUẢ KHAI PHÁ VÀO KHO (OLAM - Bài 6), để ứng dụng và Superset dùng chung.

Logic giống notebook DeThucHanh01_GiaNongSan.ipynb (nơi so sánh, chọn mô hình và tham số):
  - Dự báo: Random Forest học độ thay đổi log(giá) - log(giá lần trước), max_features = 0.5, min_samples_leaf = 1
  - Khoảng 80%: phân vị 10% / 90% sai số log trên tập kiểm tra 2025 (mô hình train đến 2024), theo cấp giá
  - Phân cụm: K-Means K = 4 trên 6 đặc trưng hành vi đã chuẩn hoá; DBSCAN (min_samples = 7, eps tại điểm gãy
    của đồ thị k-khoảng cách) đánh dấu chuỗi dị biệt

Bảng ghi vào ClickHouse (ghi đè mỗi lần chạy):
  ttns.ml_forecast  : dự báo tháng kế tiếp cho từng chuỗi còn hoạt động (+ khoảng 80%)
  ttns.ml_cluster   : đặc trưng hành vi + nhãn cụm K-Means + cờ dị biệt DBSCAN của từng chuỗi ≥ 24 tháng
  ttns.ml_metrics   : sai số trên tập kiểm tra 2025 của mô hình và các mô hình cơ sở

Chạy:  python ml/publish_ml.py     (ClickHouse ở localhost:8123)
"""
from __future__ import annotations

import io
import os
from datetime import date

import numpy as np
import pandas as pd
import requests
from sklearn.cluster import DBSCAN, KMeans
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

CH_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CH_AUTH = ("default", os.getenv("CLICKHOUSE_PASSWORD", "mypassword"))
SEED = 42
KEY = ["nhom_san_pham", "san_pham", "mat_hang", "tinh_cu", "loai_gia"]
NUM = ["log_gia_lan_truoc", "log_gia_thang_truoc", "log_tb_3_lan_truoc", "log_gia_cung_ky", "so_thang_cach",
       "bien_do_lan_truoc", "so_lan_bao_gia_truoc", "thu_tu_trong_chuoi", "year", "thang_sin", "thang_cos"]
CAT = ["nhom_san_pham", "san_pham", "cap_gia", "vung"]
CLUSTER_FEATS = ["bien_do_mua_vu", "thang_dinh_sin", "thang_dinh_cos", "do_bien_dong", "xu_huong_nam", "ty_le_dung_gia"]
K = 4


# ================================================================ ClickHouse
def query(sql: str) -> pd.DataFrame:
    r = requests.post(CH_URL, data=(sql + "\nFORMAT CSVWithNames").encode("utf-8"), auth=CH_AUTH, timeout=120)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def execute(sql: str) -> None:
    r = requests.post(CH_URL, data=sql.encode("utf-8"), auth=CH_AUTH, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(r.text[:500])


def replace_table(name: str, ddl_cols: str, order_by: str, df: pd.DataFrame) -> None:
    execute(f"DROP TABLE IF EXISTS ttns.{name}")
    execute(f"CREATE TABLE ttns.{name} ({ddl_cols}) ENGINE = MergeTree ORDER BY ({order_by})")
    r = requests.post(CH_URL, params={"query": f"INSERT INTO ttns.{name} FORMAT CSVWithNames"},
                      data=df.to_csv(index=False).encode("utf-8"), auth=CH_AUTH, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(r.text[:500])
    print(f"ttns.{name}: {len(df):,} dòng")


# ================================================================ đặc trưng (giống notebook mục 2.1)
def load() -> pd.DataFrame:
    df = query("SELECT * FROM ttns.v_price_monthly WHERE don_vi_tinh = 'VNĐ/kg'")
    df["month"] = pd.to_datetime(df["month"])
    df["sid"] = df.groupby(KEY, sort=True).ngroup()
    return df.sort_values(["sid", "month"]).reset_index(drop=True)


def add_features(d: pd.DataFrame) -> pd.DataFrame:
    d = d.sort_values(["sid", "month"]).reset_index(drop=True)
    d["log_gia"] = np.log(d["median_price"])
    g = d.groupby("sid")
    prev = g["month"].shift(1)
    d["so_thang_cach"] = (d["month"].dt.year - prev.dt.year) * 12 + d["month"].dt.month - prev.dt.month
    d["log_gia_lan_truoc"] = g["log_gia"].shift(1)
    d["log_gia_thang_truoc"] = np.where(d["so_thang_cach"] == 1, d["log_gia_lan_truoc"], np.nan)
    d["log_tb_3_lan_truoc"] = g["log_gia"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    d["bien_do_lan_truoc"] = g["max_price"].shift(1) / g["min_price"].shift(1)
    d["so_lan_bao_gia_truoc"] = g["n_obs"].shift(1)
    d["thu_tu_trong_chuoi"] = g.cumcount()
    lag12 = d[["sid", "month", "log_gia"]].copy()
    lag12["month"] = lag12["month"] + pd.DateOffset(months=12)
    d = d.drop(columns="log_gia_cung_ky", errors="ignore").merge(
        lag12.rename(columns={"log_gia": "log_gia_cung_ky"}), on=["sid", "month"], how="left")
    d["year"] = d["month"].dt.year
    d["thang_sin"] = np.sin(2 * np.pi * d["month"].dt.month / 12)
    d["thang_cos"] = np.cos(2 * np.pi * d["month"].dt.month / 12)
    return d


def next_month_rows(d: pd.DataFrame, min_last: pd.Timestamp) -> pd.DataFrame:
    """1 dòng 'tháng kế tiếp' cho mỗi chuỗi còn hoạt động; đặc trưng tính từ lịch sử như lúc huấn luyện."""
    target = d["month"].max() + pd.DateOffset(months=1)
    rows = []
    for sid, g in d.groupby("sid"):
        last = g.iloc[-1]
        if last["month"] < min_last:
            continue
        gap = (target.year - last["month"].year) * 12 + target.month - last["month"].month
        cung_ky = g.loc[g["month"] == target - pd.DateOffset(months=12), "log_gia"]
        rows.append({**{c: last[c] for c in KEY + ["cap_gia", "vung", "tinh_moi"]}, "sid": sid, "month": target,
                     "last_month": last["month"], "last_price": last["median_price"],
                     "log_gia_lan_truoc": last["log_gia"], "so_thang_cach": gap,
                     "log_gia_thang_truoc": last["log_gia"] if gap == 1 else np.nan,
                     "log_tb_3_lan_truoc": g["log_gia"].tail(3).mean(),
                     "log_gia_cung_ky": cung_ky.iloc[0] if len(cung_ky) else np.nan,
                     "bien_do_lan_truoc": last["max_price"] / last["min_price"],
                     "so_lan_bao_gia_truoc": last["n_obs"], "thu_tu_trong_chuoi": len(g), "year": target.year,
                     "thang_sin": np.sin(2 * np.pi * target.month / 12),
                     "thang_cos": np.cos(2 * np.pi * target.month / 12)})
    return pd.DataFrame(rows)


# ================================================================ mô hình dự báo
class DeltaRF:
    """RF học log(giá) - log(giá lần trước); dự báo = giá lần trước × exp(dự báo độ thay đổi)."""

    def __init__(self):
        pre = ColumnTransformer([
            ("num", Pipeline([("impute", SimpleImputer(strategy="median", add_indicator=True)),
                              ("scale", StandardScaler())]), NUM),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CAT)])
        self.pipe = Pipeline([("pre", pre), ("rf", RandomForestRegressor(
            n_estimators=300, max_features=0.5, min_samples_leaf=1, random_state=SEED, n_jobs=-1))])

    def _base(self, X):
        return X["log_gia_lan_truoc"].fillna(self.fill_).to_numpy()

    def fit(self, X, y):
        self.fill_ = float(np.median(np.log(y)))
        self.pipe.fit(X[NUM + CAT], np.log(np.asarray(y)) - self._base(X))
        return self

    def predict_log(self, X):
        return self._base(X) + self.pipe.predict(X[NUM + CAT])


def scores(y, p):
    return {"rmse": float(np.sqrt(mean_squared_error(y, p))), "mae": float(mean_absolute_error(y, p)),
            "r2": float(r2_score(y, p)), "mape": float(np.mean(np.abs(p - y) / y))}


# ================================================================ phân cụm (giống notebook mục 4)
def cluster_features(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    d["yr"] = d["month"].dt.year
    keep = d.groupby(["sid", "yr"])["log_gia"].transform("size") >= 6
    dd = d[keep].copy()
    dd["dev"] = dd["log_gia"] - dd.groupby(["sid", "yr"])["log_gia"].transform("mean")
    prof = dd.groupby(["sid", dd["month"].dt.month])["dev"].mean().unstack()
    rows = []
    for sid, g in d.groupby("sid"):
        if len(g) < 24 or sid not in prof.index or prof.loc[sid].notna().sum() < 10:
            continue
        p = prof.loc[sid]
        r = (g["log_gia"] - g["log_gia_lan_truoc"])[g["so_thang_cach"] == 1]
        t = (g["month"] - g["month"].min()).dt.days / 365.25
        peak = int(p.idxmax())
        rows.append({"sid": sid, **{c: g.iloc[0][c] for c in KEY + ["cap_gia", "vung"]},
                     "so_thang": len(g), "gia_trung_vi": float(g["median_price"].median()),
                     "bien_do_mua_vu": np.expm1(p.max()) - np.expm1(p.min()),
                     "thang_dinh": peak, "thang_dinh_sin": np.sin(2 * np.pi * peak / 12),
                     "thang_dinh_cos": np.cos(2 * np.pi * peak / 12), "do_bien_dong": r.std(),
                     "xu_huong_nam": np.expm1(np.polyfit(t, g["log_gia"], 1)[0]),
                     "ty_le_dung_gia": (r.abs() <= np.log1p(0.02)).mean()})
    return pd.DataFrame(rows)


def name_cluster(c, F):
    if c["bien_do_mua_vu"] < F["bien_do_mua_vu"].quantile(0.35) and c["do_bien_dong"] < F["do_bien_dong"].quantile(0.35):
        return "Ổn định, ít mùa vụ"
    if c["do_bien_dong"] > F["do_bien_dong"].quantile(0.75):
        return "Biến động mạnh, mùa vụ sâu" + (", giá đi xuống" if c["xu_huong_nam"] < -0.05 else "")
    return f"Mùa vụ vừa, đỉnh T{int(c['thang_dinh'])}" + (", giá tăng dần" if c["xu_huong_nam"] > 0.05 else "")


def run_clustering(d: pd.DataFrame) -> pd.DataFrame:
    F = cluster_features(d)
    X = StandardScaler().fit_transform(F[CLUSTER_FEATS])
    km = KMeans(n_clusters=K, n_init=30, random_state=SEED).fit(X)
    F["cluster_id"] = km.labels_
    centers = F.groupby("cluster_id").agg(bien_do_mua_vu=("bien_do_mua_vu", "mean"), do_bien_dong=("do_bien_dong", "mean"),
                                          xu_huong_nam=("xu_huong_nam", "mean"),
                                          thang_dinh=("thang_dinh", lambda s: s.mode().iloc[0]))
    F["cluster_name"] = F["cluster_id"].map({i: f"Cụm {i}: {name_cluster(c, F)}" for i, c in centers.iterrows()})
    min_samples = len(CLUSTER_FEATS) + 1
    kd = np.sort(NearestNeighbors(n_neighbors=min_samples).fit(X).kneighbors(X)[0][:, -1])
    xs = np.arange(len(kd))
    x1, y1, x2, y2 = 0, kd[0], len(kd) - 1, kd[-1]
    eps = float(kd[np.argmax(np.abs((y2 - y1) * xs - (x2 - x1) * kd + x2 * y1 - y2 * x1))])
    F["dbscan_label"] = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(X)
    F["is_outlier"] = (F["dbscan_label"] == -1).astype(int)
    print(f"K-Means K={K}; DBSCAN eps={eps:.2f}: {F['is_outlier'].sum()} chuỗi dị biệt / {len(F)}")
    return F


# ================================================================ main
def main():
    run_date = date.today().isoformat()
    d = add_features(load())
    last_month = d["month"].max()
    print(f"Dữ liệu: {len(d):,} dòng, {d['sid'].nunique()} chuỗi, đến {last_month:%m/%Y}")

    # 1) đánh giá trên năm cuối (train đến hết năm trước) -> độ đo + phân vị sai số cho khoảng 80%
    test_year = last_month.year
    tr, te = d[d["year"] < test_year], d[d["year"] == test_year]
    model = DeltaRF().fit(tr, tr["median_price"])
    pred_log = model.predict_log(te)
    naive = np.exp(te["log_gia_lan_truoc"].fillna(np.median(np.log(tr["median_price"]))))
    err = pd.DataFrame({"cap_gia": te["cap_gia"].values, "err": np.log(te["median_price"].values) - pred_log})
    q = err.groupby("cap_gia")["err"].quantile([0.1, 0.9]).unstack()
    metrics = pd.DataFrame([
        {"run_date": run_date, "model": "RF học độ thay đổi", "test_year": test_year, **scores(te["median_price"], np.exp(pred_log))},
        {"run_date": run_date, "model": "Cơ sở: giữ nguyên giá", "test_year": test_year, **scores(te["median_price"], naive)},
    ])
    print(metrics[["model", "rmse", "mae", "r2", "mape"]].round(3).to_string(index=False))

    # 2) mô hình cuối: train trên toàn bộ, dự báo tháng kế tiếp cho các chuỗi có báo giá trong 6 tháng gần nhất
    final = DeltaRF().fit(d, d["median_price"])
    fut = next_month_rows(d, min_last=last_month - pd.DateOffset(months=5))
    fl = final.predict_log(fut)
    lo = fut["cap_gia"].map(q[0.1]).fillna(err["err"].quantile(0.1))
    hi = fut["cap_gia"].map(q[0.9]).fillna(err["err"].quantile(0.9))
    forecast = fut[KEY + ["cap_gia", "tinh_moi", "vung"]].assign(
        run_date=run_date, target_month=fut["month"].dt.strftime("%Y-%m-%d"),
        last_month=fut["last_month"].dt.strftime("%Y-%m-%d"), last_price=fut["last_price"],
        yhat=np.exp(fl).round(0), yhat_lo=np.exp(fl + lo).round(0), yhat_hi=np.exp(fl + hi).round(0),
        model="RF học độ thay đổi")
    forecast["pct_vs_last"] = (forecast["yhat"] / forecast["last_price"] - 1).round(4)

    # 3) phân cụm + dị biệt
    clusters = run_clustering(d).assign(run_date=run_date)

    S = "String"
    replace_table("ml_forecast",
                  "nhom_san_pham String, san_pham String, mat_hang String, tinh_cu String, loai_gia String, "
                  "cap_gia String, tinh_moi String, vung String, run_date Date, target_month Date, last_month Date, "
                  "last_price Float64, yhat Float64, yhat_lo Float64, yhat_hi Float64, model String, pct_vs_last Float64",
                  "san_pham, mat_hang, tinh_cu, loai_gia", forecast)
    cl_cols = KEY + ["cap_gia", "vung", "so_thang", "gia_trung_vi"] + CLUSTER_FEATS + \
        ["thang_dinh", "cluster_id", "cluster_name", "dbscan_label", "is_outlier", "run_date"]
    types = {c: S for c in KEY + ["cap_gia", "vung", "cluster_name"]} | {"run_date": "Date", "so_thang": "UInt16",
             "thang_dinh": "UInt8", "cluster_id": "UInt8", "dbscan_label": "Int16", "is_outlier": "UInt8"}
    replace_table("ml_cluster", ", ".join(f"{c} {types.get(c, 'Float64')}" for c in cl_cols),
                  "cluster_id, san_pham, mat_hang, tinh_cu", clusters[cl_cols])
    replace_table("ml_metrics", "run_date Date, model String, test_year UInt16, rmse Float64, mae Float64, "
                  "r2 Float64, mape Float64", "model", metrics)


if __name__ == "__main__":
    main()
