"""
DAG: ttns_price_pipeline
Luồng:
  ensure_buckets -> extract -> check_changed -> transform -> prepare_clickhouse -> cdc -> validate -> load_clickhouse
  -> build_olap_cubes -> commit_cdc

  extract        : tải file Excel gốc (hoặc dùng lại raw đã lưu nếu có replay_dt) -> raw-data/ttns/dt=.../*.xlsx, tính md5 dữ liệu
  check_changed  : md5 dữ liệu giống lần commit gần nhất -> nguồn chưa cập nhật -> DỪNG, các task sau bị skip
  transform      : làm sạch -> processed-data/ttns/dt=.../*.csv
  prepare_ch     : tạo/migrate bảng ClickHouse; nếu bảng vừa tạo lại hoặc rỗng -> nạp lại toàn bộ (full)
  cdc            : so fact_price_monthly với snapshot ngày trước theo khoá (mặt hàng, tỉnh, tháng, loại giá, đơn vị)
                   -> cdc_log.csv, fact_upsert.csv, price_clean_upsert.csv
                   -> processed-data/ttns/_cdc/<scope>/dt=.../snapshot.csv
  load           : chỉ INSERT phần thay đổi (ReplacingMergeTree version/is_deleted), price_clean xoá + ghi theo khoá đổi
  build_cubes    : dựng lại 3 khối con OLAP (AggregatingMergeTree) chọn bằng thuật toán tham lam
  commit_cdc     : ghi _meta.json (kèm md5) -> mốc cho lần sau

ClickHouse (database ttns): dim_product, dim_market, dim_date, price_clean, fact_price_monthly (đọc bằng FINAL),
  cdc_log (initial/new_month/backfill/revision/removed), view v_price_monthly, v_changes

Code xử lý: pipelines/ttns (scraper, cleaning, cdc, schema) + pipelines/common.
"""
from __future__ import annotations

import hashlib
import io
import logging
import re
import time
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from pipelines.common import cdc as cdc_store
from pipelines.common import clickhouse_helper as ch
from pipelines.common import minio_helper as mh
from pipelines.common.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, PROCESSED_BUCKET, RAW_BUCKET, TZ
from pipelines.ttns import cdc as ttns_cdc
from pipelines.ttns import olap, schema, scraper
from pipelines.ttns.cleaning import BUILD_TABLES, build_tables, clean

log = logging.getLogger(__name__)

SOURCE_PREFIX = "ttns"
DIM_TABLES = ["dim_product", "dim_market", "dim_price_type", "dim_date"]
KEY_TUPLE = "(" + ", ".join(ttns_cdc.KEYS) + ")"


def _s3(key: str, structure: str) -> str:
    """Biểu thức s3() để dùng CSV trên MinIO trong câu SQL (vd làm danh sách khoá cho DELETE)."""
    url = f"http://{MINIO_ENDPOINT}/{PROCESSED_BUCKET}/{key}"
    return f"s3('{url}', '{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}', 'CSVWithNames', '{structure}')"


KEY_STRUCTURE = "product_id UInt32, market_id UInt32, date_id UInt32, loai_gia String, don_vi_tinh String"


@dag(
    dag_id="ttns_price_pipeline",
    description="thitruongnongsan.gov.vn (giá rau quả trong nước) -> MinIO raw/processed -> CDC -> ClickHouse",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    schedule="0 6 * * 1",  # 6h sáng thứ Hai hằng tuần (dữ liệu nguồn cập nhật theo tháng)
    catchup=False,
    params={
        "products": Param(
            [],
            type="array",
            description="Tên sản phẩm đúng như trên web (vd Vải, Xoài). Để trống = tải tất cả trong 1 file",
        ),
        "force": Param(False, type="boolean", description="Chạy tiếp dù file không đổi / dù xoá nhiều bất thường"),
        "replay_dt": Param(
            None, type=["null", "string"],
            description="YYYY-MM-DD: xử lý lại raw đã lưu của ngày này, không tải lại từ web",
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ttns", "nong-san", "minio", "clickhouse", "etl", "cdc"],
)
def ttns_price_pipeline():
    @task
    def ensure_buckets() -> None:
        mh.ensure_buckets(RAW_BUCKET, PROCESSED_BUCKET)

    @task
    def extract(**context) -> dict:
        """Tải file Excel gốc từ web, ghi nguyên vẹn vào raw-data. Trả về md5 dữ liệu để phát hiện nguồn có đổi không."""
        params = context["params"]
        products = sorted(p for p in (params["products"] or []) if p)
        replay_dt = params.get("replay_dt")
        today = replay_dt or pendulum.now(TZ).format("YYYY-MM-DD")
        client = mh.client()
        session = None if replay_dt else scraper.session()
        keys, md5s = [], []
        for product in products or [""]:
            name = re.sub(r"\s+", "_", product) if product else "all"
            key = f"{SOURCE_PREFIX}/dt={today}/cat{scraper.CAT_ID}_item{scraper.ITEM_NO}_{name}.xlsx"
            if replay_dt:
                content = mh.get_bytes(client, RAW_BUCKET, key)
            else:
                content = scraper.scrape_excel(session, product)
                mh.put_bytes(client, RAW_BUCKET, key, content, scraper.XLSX_TYPE)
                time.sleep(1)  # lịch sự với server
            keys.append(key)
            # Hash DỮ LIỆU chứ không hash file: mỗi lần xuất, web nén zip lại với thời gian mới nên byte luôn khác
            data_csv = pd.read_excel(io.BytesIO(content)).to_csv(index=False)
            md5s.append(hashlib.md5(data_csv.encode("utf-8")).hexdigest())
        return {
            "date": today, "keys": keys, "scope": cdc_store.scope_name(products),
            "content_md5": hashlib.md5("".join(md5s).encode()).hexdigest(),
        }

    @task.short_circuit
    def check_changed(extracted: dict, **context) -> bool:
        """False -> Airflow skip toàn bộ task phía sau (không làm gì vì nguồn chưa cập nhật)."""
        if context["params"]["force"]:
            return True
        client = mh.client()
        dts = [d for d in cdc_store.committed_dts(client, SOURCE_PREFIX, extracted["scope"]) if d <= extracted["date"]]
        if not dts:
            return True
        last = cdc_store.read_meta(client, SOURCE_PREFIX, extracted["scope"], dts[-1])
        if last.get("content_md5") == extracted["content_md5"]:
            log.info("Dữ liệu giống hệt lần commit %s (md5 %s) -> nguồn chưa cập nhật, dừng.", dts[-1], extracted["content_md5"])
            return False
        return True

    @task
    def transform(extracted: dict) -> dict:
        client = mh.client()
        parts, all_stats = [], []
        for key in extracted["keys"]:
            raw = pd.read_excel(io.BytesIO(mh.get_bytes(client, RAW_BUCKET, key)))
            df, stats = clean(raw, key.rsplit("/", 1)[-1])
            log.info("%s: %s", key, stats)
            parts.append(df)
            all_stats.append(stats)
        if not parts or sum(len(p) for p in parts) == 0:
            raise ValueError("Không có dòng dữ liệu hợp lệ nào sau khi làm sạch")
        frames = build_tables(pd.concat(parts, ignore_index=True))

        prefix = f"{SOURCE_PREFIX}/dt={extracted['date']}"
        rows = {t: mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/{t}.csv", df) for t, df in frames.items()}
        log.info("Số dòng mỗi bảng: %s", rows)
        return {**extracted, "prefix": prefix, "rows": rows, "raw_keys": extracted["keys"], "stats": all_stats}

    @task
    def prepare_clickhouse() -> bool:
        """Tạo/migrate bảng. Trả về True nếu kho đang trống (vừa tạo lại / mất dữ liệu) -> cần nạp lại toàn bộ."""
        ch.ensure_schema(schema.DB, schema.TABLES, schema.VIEWS, rebuildable=schema.REBUILDABLE)
        return ch.count(schema.DB, "fact_price_monthly") == 0

    @task
    def cdc(result: dict, warehouse_empty: bool) -> dict:
        client = mh.client()
        scope, prefix = result["scope"], result["prefix"]
        fact = mh.get_csv(client, PROCESSED_BUCKET, f"{prefix}/fact_price_monthly.csv", dtype=ttns_cdc.SNAPSHOT_DTYPE)

        base = cdc_store.base_dt(client, SOURCE_PREFIX, scope, result["date"])
        full = warehouse_empty and scope == "all"
        if full and base:
            log.warning("Kho ClickHouse trống nhưng đã có mốc %s -> bỏ mốc, nạp lại toàn bộ", base)
            base = None
        prev = cdc_store.load_snapshot(client, SOURCE_PREFIX, scope, base, dtype=ttns_cdc.SNAPSHOT_DTYPE)

        version = int(pendulum.now(TZ).format("YYYYMMDDHHmmss"))
        cdc_log, fact_upsert, snapshot, stats = ttns_cdc.compute(prev, fact, result["date"], scope, version)
        log.info("CDC scope=%s so với mốc %s: %s", scope, base, stats)

        clean_df = mh.get_csv(client, PROCESSED_BUCKET, f"{prefix}/price_clean.csv", dtype=ttns_cdc.SNAPSHOT_DTYPE)
        prev_groups = cdc_store.load_snapshot(client, SOURCE_PREFIX, scope, base, ttns_cdc.SNAPSHOT_DTYPE, name="snapshot_clean")
        clean_keys, clean_upsert, groups = ttns_cdc.clean_changes(prev_groups, clean_df)

        mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/cdc_log.csv", cdc_log)
        mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/fact_upsert.csv", fact_upsert)
        mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/price_clean_changed_keys.csv", clean_keys)
        mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/price_clean_upsert.csv", clean_upsert)
        cdc_store.save_snapshot(client, SOURCE_PREFIX, scope, result["date"], snapshot)
        cdc_store.save_snapshot(client, SOURCE_PREFIX, scope, result["date"], groups, name="snapshot_clean")
        return {**result, "cdc": {
            "base_dt": base, "full_reload": base is None, "version": version, "stats": stats,
            "rows": {
                "cdc_log": len(cdc_log), "fact_upsert": len(fact_upsert),
                "price_clean_changed_groups": len(clean_keys), "price_clean_upsert": len(clean_upsert),
            },
        }}

    @task
    def validate(result: dict, **context) -> None:
        client = mh.client()
        for key in result["raw_keys"]:
            client.stat_object(RAW_BUCKET, key)
        for table in BUILD_TABLES:
            stat = client.stat_object(PROCESSED_BUCKET, f"{result['prefix']}/{table}.csv")
            if result["rows"][table] == 0:
                raise ValueError(f"Bảng {table} rỗng")
            log.info("OK %s: %d dòng, %d bytes", table, result["rows"][table], stat.size)
        for s in result["stats"]:
            if s["rows_clean"] < 0.9 * s["rows_raw"]:
                raise ValueError(f"Làm sạch loại bỏ quá nhiều dòng: {s}")
        ratio = result["cdc"]["stats"]["delete_ratio"]
        if ratio > ttns_cdc.MAX_DELETE_RATIO and not context["params"]["force"]:
            raise ValueError(
                f"{ratio:.0%} khoá biến mất so với mốc {result['cdc']['base_dt']} - bất thường (export lỗi? đổi tên?). "
                "Kiểm tra cdc_log.csv; nếu đúng là nguồn đã xoá thì chạy lại với force=true."
            )

    @task
    def load_clickhouse(result: dict) -> None:
        db, prefix, scope, run_date = schema.DB, result["prefix"], result["scope"], result["date"]
        full = result["cdc"]["full_reload"] and scope == "all"
        if full:
            for table in schema.REBUILDABLE:
                ch.query(f"TRUNCATE TABLE {db}.{table}")
            log.info("Nạp lại toàn bộ (chưa có mốc CDC)")

        # dim nhỏ: ghi đè toàn bộ, ReplacingMergeTree tự gộp trùng
        for table in DIM_TABLES:
            ch.load_csv_from_s3(db, table, schema.TABLES[table][0], f"{prefix}/{table}.csv")

        # fact: chỉ INSERT dòng thay đổi; bản version lớn nhất thắng, D có is_deleted=1
        ch.load_csv_from_s3(db, "fact_price_monthly", schema.TABLES["fact_price_monthly"][0], f"{prefix}/fact_upsert.csv")

        # price_clean (không có khoá riêng từng dòng): xoá mọi dòng của nhóm có thay đổi rồi ghi lại bản mới
        if not full and result["cdc"]["rows"]["price_clean_changed_groups"]:
            ch.query(
                f"DELETE FROM {db}.price_clean WHERE {KEY_TUPLE} IN "
                f"(SELECT {', '.join(ttns_cdc.KEYS)} FROM {_s3(prefix + '/price_clean_changed_keys.csv', KEY_STRUCTURE)})"
            )
        ch.load_csv_from_s3(db, "price_clean", schema.TABLES["price_clean"][0], f"{prefix}/price_clean_upsert.csv")

        # nhật ký: chạy lại cùng ngày/scope cho cùng kết quả -> xoá rồi ghi lại
        ch.query(f"DELETE FROM {db}.cdc_log WHERE run_date = '{run_date}' AND scope = '{scope}'")
        ch.load_csv_from_s3(db, "cdc_log", schema.TABLES["cdc_log"][0], f"{prefix}/cdc_log.csv")

        # Đối soát: trong phạm vi sản phẩm của lần chạy, kho phải khớp đúng snapshot mới
        in_scope = f"product_id IN (SELECT product_id FROM {_s3(prefix + '/dim_product.csv', schema.TABLES['dim_product'][0])})"
        checks = {
            "fact_price_monthly": (
                int(ch.query(f"SELECT count() FROM {db}.fact_price_monthly FINAL WHERE is_deleted = 0 AND {in_scope}")),
                result["rows"]["fact_price_monthly"],
            ),
            "price_clean": (ch.count(db, "price_clean", in_scope), result["rows"]["price_clean"]),
            "cdc_log": (ch.count(db, "cdc_log", f"run_date = '{run_date}' AND scope = '{scope}'"), result["cdc"]["rows"]["cdc_log"]),
        }
        for table, (loaded, expected) in checks.items():
            if loaded != expected:
                raise ValueError(f"{table}: ClickHouse có {loaded} dòng, mong đợi {expected}")
        log.info("✅ ClickHouse OK: %s", {t: v[0] for t, v in checks.items()})

    @task
    def build_olap_cubes() -> dict:
        """Dựng lại các khối con vật chất hoá (pipelines/ttns/olap.py) từ dữ liệu vừa nạp."""
        sizes = {}
        for name, spec in olap.CUBOIDS.items():
            for sql in olap.build_sql(name, spec):
                ch.query(sql)
            sizes[name] = ch.count(schema.DB, name)
            if sizes[name] == 0:
                raise ValueError(f"Khối con {name} rỗng")
        log.info("Khối con OLAP: %s", sizes)
        return sizes

    @task
    def commit_cdc(result: dict) -> None:
        cdc_store.commit(mh.client(), SOURCE_PREFIX, result["scope"], result["date"], {
            "committed_at": pendulum.now(TZ).isoformat(), "content_md5": result["content_md5"],
            "raw_keys": result["raw_keys"], **result["cdc"],
        })

    buckets = ensure_buckets()
    extracted = extract()
    buckets >> extracted
    gate = check_changed(extracted)
    transformed = transform(extracted)
    gate >> transformed
    prepared = prepare_clickhouse()
    gate >> prepared
    changed = cdc(transformed, prepared)
    loaded = load_clickhouse(changed)
    validate(changed) >> loaded >> build_olap_cubes() >> commit_cdc(changed)


ttns_price_pipeline()
