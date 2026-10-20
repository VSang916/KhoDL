"""
Thử nghiệm dự báo giá tháng tới (h = 1) trên ttns.v_price_monthly bằng LightGBM "global model"
(1 mô hình học chung cho mọi chuỗi), so với các mốc đơn giản. Backtest rolling-origin: với mỗi tháng test,
chỉ train trên dữ liệu TRƯỚC tháng đó.

Chạy từ máy host (ClickHouse map cổng 8123):
    pip install pandas requests lightgbm
    python ml/forecast_lgbm.py                 # test 6 tháng cuối
    python ml/forecast_lgbm.py --test-months 12

Chuỗi = (nhom_san_pham, san_pham, mat_hang, tinh_cu, loai_gia, don_vi_tinh), giá = median_price.
Target = log(giá_t) - log(giá_{t-1})  (dự báo % thay đổi, không dự báo mức giá).

Kết quả (dữ liệu 04/2021 - 12/2025, chạy 19/10/2026):
                      MAPE    đúng chiều (tăng/giảm/ngang ±2%)
  6 tháng cuối naive  12.7%   35%
               lgbm   12.9%   40%
  12 tháng     naive  13.4%   35%
               lgbm   13.5%   41%
-> Chỉ với lịch sử giá theo tháng, LightGBM KHÔNG thắng "giá tháng sau = giá tháng này" (random walk).
   Thêm feature mùa vụ của biến động và biến động cùng sản phẩm ở nơi khác cũng không cải thiện.
   Muốn thắng cần dữ liệu ngoài (thời tiết, lịch lễ Tết, xuất khẩu, giá xăng...) - xem README phần học máy.
"""
from __future__ import annotations

import argparse
import io
import os

import lightgbm as lgb
import numpy as np
import pandas as pd
import requests

CH_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CH_AUTH = (os.getenv("CLICKHOUSE_USER", "default"), os.getenv("CLICKHOUSE_PASSWORD", "mypassword"))

KEY = ["nhom_san_pham", "san_pham", "mat_hang", "tinh_cu", "loai_gia", "don_vi_tinh"]
CATS = ["nhom_san_pham", "san_pham", "tinh_moi", "cap_gia", "don_vi_tinh"]
LAGS = [1, 2, 3, 6, 12]


def load() -> pd.DataFrame:
    sql = f"""SELECT {', '.join(KEY)}, tinh_moi, cap_gia, month, median_price, n_obs
              FROM ttns.v_price_monthly FORMAT CSVWithNames"""
    resp = requests.post(CH_URL, data=sql.encode("utf-8"), auth=CH_AUTH, timeout=120)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), parse_dates=["month"])


def make_features(df: pd.DataFrame) -> pd.DataFrame:
    """Lưới tháng đầy đủ cho từng chuỗi (tháng thiếu = NaN) rồi tạo lag. Mọi feature chỉ dùng quá khứ."""
    df = df.copy()
    df["sid"] = df.groupby(KEY, sort=False).ngroup()
    attrs = df.drop_duplicates("sid").set_index("sid")[KEY + ["tinh_moi", "cap_gia"]]
    frames = []
    for sid, g in df.groupby("sid"):
        idx = pd.date_range(g["month"].min(), g["month"].max(), freq="MS")
        frames.append(g.set_index("month")[["median_price", "n_obs"]].reindex(idx).rename_axis("month")
                      .reset_index().assign(sid=sid))
    full = pd.concat(frames, ignore_index=True).join(attrs, on="sid")

    full["y"] = np.log(full["median_price"])
    grp = full.groupby("sid")["y"]
    for k in LAGS:
        full[f"lag{k}"] = grp.shift(k)
    full["n_obs_lag1"] = full.groupby("sid")["n_obs"].shift(1)
    full["ret1"] = full["lag1"] - full["lag2"]                       # % đổi tháng trước
    full["ret12"] = full["lag1"] - full["lag12"]                     # so với cùng kỳ năm trước
    full["roll_mean3"] = grp.transform(lambda s: s.shift(1).rolling(3, min_periods=2).mean()) - full["lag1"]
    full["roll_std6"] = grp.transform(lambda s: s.shift(1).diff().rolling(6, min_periods=3).std())
    full["month_sin"] = np.sin(2 * np.pi * full["month"].dt.month / 12)
    full["month_cos"] = np.cos(2 * np.pi * full["month"].dt.month / 12)
    full["age"] = full.groupby("sid").cumcount()                     # chuỗi đã có bao nhiêu tháng
    full["target"] = full["y"] - full["lag1"]
    # mùa vụ của BIẾN ĐỘNG: cùng tháng năm trước / 2 năm trước giá đã tăng hay giảm bao nhiêu
    tgt = full.groupby("sid")["target"]
    full["season_ret1"] = tgt.shift(12)
    full["season_ret2"] = tgt.shift(24)
    # tín hiệu thị trường: tháng trước các chuỗi cùng sản phẩm (tỉnh khác, cấp giá khác) biến động trung bình bao nhiêu
    prod_ret = full.groupby(["san_pham", "month"])["target"].transform("mean")
    full["_prod_ret"] = prod_ret
    full["market_ret1"] = full.groupby("sid")["_prod_ret"].shift(1)
    full = full.drop(columns="_prod_ret")
    for c in CATS:
        full[c] = full[c].astype("category")
    return full


FEATURES = ([f"lag{k}" for k in LAGS[1:]] + ["n_obs_lag1", "ret1", "ret12", "roll_mean3", "roll_std6",
            "month_sin", "month_cos", "age", "season_ret1", "season_ret2", "market_ret1"] + CATS)


def backtest(full: pd.DataFrame, test_months: int) -> pd.DataFrame:
    usable = full.dropna(subset=["target", "lag1"]).copy()
    # lag2..lag12 tính tương đối so với lag1 để mô hình học hình dạng, không học mức giá tuyệt đối
    for k in LAGS[1:]:
        usable[f"lag{k}"] = usable[f"lag{k}"] - usable["lag1"]
    months = sorted(usable["month"].unique())[-test_months:]
    out = []
    for m in months:
        train, test = usable[usable["month"] < m], usable[usable["month"] == m]
        model = lgb.LGBMRegressor(objective="l1", n_estimators=400, learning_rate=0.03, num_leaves=15,
                                  min_child_samples=30, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                                  verbose=-1, random_state=0)
        model.fit(train[FEATURES], train["target"])
        pred = test[KEY + ["cap_gia", "month", "median_price", "lag1"]].copy()
        pred["lgbm"] = np.exp(test["lag1"] + model.predict(test[FEATURES]))
        pred["naive"] = np.exp(test["lag1"])
        out.append(pred)
    return pd.concat(out, ignore_index=True)


def report(res: pd.DataFrame) -> None:
    def mape(col, d):
        return ((d[col] - d["median_price"]).abs() / d["median_price"]).mean()

    def direction_acc(col, d):  # đoán đúng chiều (tăng / giảm / đi ngang ±2%)
        def cls(x):
            return np.select([x > 0.02, x < -0.02], [1, -1], 0)
        true = cls(d["median_price"] / d["naive"] - 1)
        return (cls(d[col] / d["naive"] - 1) == true).mean()

    print(f"Điểm test: {len(res)}  ({res['month'].min():%m/%Y} → {res['month'].max():%m/%Y})")
    print(f"{'':10s}{'MAPE':>8s}{'APE trung vị':>14s}{'đúng chiều':>12s}")
    for col in ("naive", "lgbm"):
        ape = (res[col] - res["median_price"]).abs() / res["median_price"]
        print(f"{col:10s}{mape(col, res):8.1%}{ape.median():14.1%}{direction_acc(col, res):12.0%}")
    for c, d in res.groupby("cap_gia", observed=True):
        print(f"  {c:9s} naive {mape('naive', d):6.1%}   lgbm {mape('lgbm', d):6.1%}   (n={len(d)})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--test-months", type=int, default=6)
    args = ap.parse_args()
    report(backtest(make_features(load()), args.test_months))
