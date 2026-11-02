from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from pipelines.common import cdc as cdc_store
from pipelines.common import clickhouse_helper as ch
from pipelines.common import minio_helper as mh
from pipelines.common.cdc import row_hash
from pipelines.common.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, PROCESSED_BUCKET, RAW_BUCKET, TZ
from pipelines.ttns import cdc as ttns_cdc
from pipelines.ttns import olap, schema, scraper
from pipelines.ttns.cleaning import BUILD_TABLES, build_tables, clean
from pipelines.ttns.export import read_raw

log = logging.getLogger(__name__)

SOURCE_PREFIX = "ttns"
DIM_TABLES = ["dim_product", "dim_market", "dim_price_type", "dim_date"]
KEY_TUPLE = "(" + ", ".join(ttns_cdc.KEYS) + ")"


def _s3(key: str, structure: str) -> str:
    """Biểu thức s3() để dùng CSV trên MinIO trong câu SQL (vd làm danh sách khoá cho DELETE)."""
    url = f"http://{MINIO_ENDPOINT}/{PROCESSED_BUCKET}/{key}"
    return f"s3('{url}', '{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}', 'CSVWithNames', '{structure}')"


KEY_STRUCTURE = "product_id UInt32, market_id UInt32, date_id UInt32, loai_gia String, don_vi_tinh String"


def _warehouse(sql: str) -> pd.DataFrame:
    """Đọc 1 bảng nhỏ của kho về DataFrame, cùng cách đọc như CSV trên MinIO để hash so sánh được."""
    text = ch.query(sql + " FORMAT CSVWithNames")
    return pd.read_csv(io.StringIO(text), dtype=ttns_cdc.SNAPSHOT_DTYPE, keep_default_na=False, na_values=[""])


@dag(
    dag_id="ttns_price_pipeline",
    description="thitruongnongsan.gov.vn/nguonwmy (giá rau quả theo tháng) -> MinIO raw/processed -> CDC -> ClickHouse",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    schedule="0 6 * * 1",
    catchup=False,
    params={
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
        """Mỗi nhóm sản phẩm: ghi nguyên vẹn file xuất (.xls) và danh sách Sản phẩm (.json) vào raw-data.
        Trả về md5 dữ liệu để phát hiện nguồn có đổi không."""
        replay_dt = context["params"].get("replay_dt")
        now = pendulum.parse(replay_dt, tz=TZ) if replay_dt else pendulum.now(TZ)
        today = now.format("YYYY-MM-DD")
        client = mh.client()
        prefix = f"{SOURCE_PREFIX}/dt={today}/"
        if replay_dt:
            keys = mh.list_keys(client, RAW_BUCKET, prefix)
            # raw trước 11/2026 là file .xlsx của trang cũ; cùng ngày có cả 2 loại thì dùng trang mới
            files = [{"key": k, "group": k[len(prefix):-4].replace("_", " "), "products": k[:-4] + ".json"}
                     for k in keys if k.endswith(".xls")] or [{"key": k} for k in keys if k.endswith(".xlsx")]
            if not files:
                raise ValueError(f"Không có raw nào cho dt={replay_dt}")
        else:
            session, files = scraper.session(), []
            for group in scraper.GROUPS:
                key = prefix + group.replace(" ", "_") + ".xls"
                content, products = scraper.scrape_group(session, group, now.format("DD/MM/YYYY"))
                mh.put_bytes(client, RAW_BUCKET, key, content, scraper.XLS_TYPE)
                mh.put_bytes(client, RAW_BUCKET, key[:-4] + ".json",
                             json.dumps(products, ensure_ascii=False).encode("utf-8"), "application/json")
                files.append({"key": key, "group": group, "products": key[:-4] + ".json"})
                time.sleep(1)  # lịch sự với server
        # Hash DỮ LIỆU đã đọc ra (sắp xếp) chứ không hash file, để không phụ thuộc thứ tự dòng hay cách web đóng gói
        md5s = []
        for f in files:
            raw = read_raw(f, lambda k: mh.get_bytes(client, RAW_BUCKET, k))
            md5s.append(hashlib.md5(raw.sort_values(by=list(raw.columns)).to_csv(index=False).encode("utf-8")).hexdigest())
        return {
            "date": today, "files": files, "scope": cdc_store.scope_name([]),
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
        for f in extracted["files"]:
            raw = read_raw(f, lambda k: mh.get_bytes(client, RAW_BUCKET, k))
            df, stats = clean(raw, f["key"].rsplit("/", 1)[-1])
            log.info("%s: %s", f["key"], stats)
            parts.append(df)
            all_stats.append(stats)
        if not parts or sum(len(p) for p in parts) == 0:
            raise ValueError("Không có dòng dữ liệu hợp lệ nào sau khi làm sạch")
        frames = build_tables(pd.concat(parts, ignore_index=True))

        prefix = f"{SOURCE_PREFIX}/dt={extracted['date']}"
        rows = {t: mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/{t}.csv", df) for t, df in frames.items()}
        log.info("Số dòng mỗi bảng: %s", rows)
        raw_keys = [k for f in extracted["files"] for k in (f["key"], f.get("products")) if k]
        return {**extracted, "prefix": prefix, "rows": rows, "raw_keys": raw_keys, "stats": all_stats}

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
        # Nhật ký (cdc_log) so với mốc hôm trước; còn dòng cần ghi thì so với TRẠNG THÁI THẬT của kho. Hai thứ khác
        # nhau khi chạy lại trong ngày (kho đã chứa lần chạy trước) hoặc sau một lần nạp hỏng giữa chừng.
        cdc_log, _, snapshot, stats = ttns_cdc.compute(prev, fact, result["date"], scope, version)
        log.info("CDC scope=%s so với mốc %s: %s", scope, base, stats)
        wh_fact = None if full else _warehouse(f"SELECT {', '.join(ttns_cdc.FACT_COLS)} FROM {schema.DB}.fact_price_monthly "
                                               "FINAL WHERE is_deleted = 0")
        if wh_fact is not None:
            wh_fact["row_hash"] = row_hash(wh_fact, ttns_cdc.TRACKED)
        _, fact_upsert, _, _ = ttns_cdc.compute(wh_fact, fact, result["date"], scope, version)

        clean_df = mh.get_csv(client, PROCESSED_BUCKET, f"{prefix}/price_clean.csv", dtype=ttns_cdc.SNAPSHOT_DTYPE)
        wh_groups = None if full else ttns_cdc.clean_groups(_warehouse(f"SELECT * FROM {schema.DB}.price_clean"))
        clean_keys, clean_upsert, groups = ttns_cdc.clean_changes(wh_groups, clean_df)

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
