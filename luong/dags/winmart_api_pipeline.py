"""
DAG: winmart_api_pipeline
Luồng:
  ensure_buckets -> extract -> transform -> cdc -> validate -> load_clickhouse -> commit_cdc

  extract   : gọi API (hoặc dùng lại raw đã lưu nếu có replay_dt) -> raw-data/winmart_api/dt=.../*.json
  transform : JSON -> 4 bảng CSV            -> processed-data/winmart_api/dt=.../*.csv
  cdc       : so trạng thái giá hôm nay với snapshot ngày trước (khoá item_no + store_code)
              -> processed-data/winmart_api/dt=.../price_changes.csv
              -> processed-data/winmart_api/_cdc/all/dt=.../snapshot.csv
  load      : fact_price (snapshot theo ngày) + price_changes (chỉ thay đổi) vào ClickHouse
  commit_cdc: ghi _meta.json -> snapshot hôm nay thành mốc cho lần sau (chỉ khi load thành công)

ClickHouse (database winmart):
  dim_product, dim_market, dim_date, fact_price  : giá của từng ngày chạy
  price_changes                                  : nhật ký thay đổi (new_item, price_up/down, promo_start/end, removed)
  v_price_daily, v_price_history (SCD2: mỗi mức giá có valid_from/valid_to)

Code xử lý: pipelines/winmart (api, transform, cdc, schema) + pipelines/common.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from pipelines.common import cdc as cdc_store
from pipelines.common import clickhouse_helper as ch
from pipelines.common import minio_helper as mh
from pipelines.common.config import PROCESSED_BUCKET, RAW_BUCKET, TZ
from pipelines.winmart import api, schema
from pipelines.winmart import cdc as winmart_cdc
from pipelines.winmart.transform import build_tables

log = logging.getLogger(__name__)

# ================================================================ CẤU HÌNH
STORES = [
    {"store_code": "1535", "store_group_code": "1998", "market_name": "WinMart 1535"},
]

# slug danh mục: copy từ tham số slug=... trong URL API ở F12. Thêm các danh mục khác vào đây
CATEGORY_SLUGS = [
    "rau-cu-trai-cay--c02",
]

SOURCE_PREFIX = "winmart_api"
CDC_SCOPE = "all"


# ================================================================ DAG
@dag(
    dag_id="winmart_api_pipeline",
    description="Winmart API -> MinIO raw/processed -> CDC -> ClickHouse",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    schedule="0 7 * * *",  # 7h sáng mỗi ngày
    catchup=False,
    params={
        "page_size": Param(18, type="integer", minimum=1, maximum=100, description="Số sản phẩm mỗi trang"),
        "replay_dt": Param(
            None, type=["null", "string"],
            description="YYYY-MM-DD: xử lý lại raw đã lưu của ngày này, không gọi API (backfill / sửa lỗi)",
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["winmart", "minio", "clickhouse", "etl", "cdc"],
)
def winmart_api_pipeline():
    @task
    def ensure_buckets() -> None:
        mh.ensure_buckets(RAW_BUCKET, PROCESSED_BUCKET)

    @task
    def extract(**context) -> dict:
        """Gọi API từng (cửa hàng, danh mục), ghi response gốc vào raw-data."""
        client = mh.client()
        replay_dt = context["params"].get("replay_dt")
        if replay_dt:
            keys = mh.list_keys(client, RAW_BUCKET, f"{SOURCE_PREFIX}/dt={replay_dt}/")
            if not keys:
                raise ValueError(f"Không có raw nào cho dt={replay_dt}")
            log.info("Replay %d file raw của %s", len(keys), replay_dt)
            return {"date": replay_dt, "keys": keys}

        page_size = int(context["params"]["page_size"])
        now = datetime.fromisoformat(pendulum.now(TZ).format("YYYY-MM-DDTHH:mm:ss"))  # giờ Việt Nam, không múi giờ
        today = now.strftime("%Y-%m-%d")
        session = api.session()
        keys = []

        for store in STORES:
            for slug in CATEGORY_SLUGS:
                pages = api.fetch_category(session, store, slug, page_size)
                n_items = sum(len(api.items_of(p)) for p in pages)
                if n_items == 0:
                    raise ValueError(
                        f"Danh mục {slug} (store {store['store_code']}) không có sản phẩm nào. "
                        "Kiểm tra lại slug/storeCode hoặc header trong F12."
                    )
                doc = {
                    "meta": {
                        "source": "winmart_api",
                        "store_code": store["store_code"],
                        "store_group_code": store["store_group_code"],
                        "market_name": store["market_name"],
                        "category_slug": slug,
                        "page_size": page_size,
                        "captured_at": now.isoformat(),
                        "item_count": n_items,
                    },
                    "pages": pages,  # response gốc của API, không chỉnh sửa
                }
                key = f"{SOURCE_PREFIX}/dt={today}/store={store['store_code']}/{slug}.json"
                mh.put_bytes(client, RAW_BUCKET, key, json.dumps(doc, ensure_ascii=False).encode("utf-8"), "application/json")
                keys.append(key)
                log.info("%s: %d sản phẩm", key, n_items)
        return {"date": today, "keys": keys}

    @task
    def transform(extracted: dict) -> dict:
        client = mh.client()
        docs = [json.loads(mh.get_bytes(client, RAW_BUCKET, k)) for k in extracted["keys"]]
        frames, stats = build_tables(docs)
        log.info("Thống kê làm sạch: %s", stats)

        prefix = f"{SOURCE_PREFIX}/dt={extracted['date']}"
        rows = {t: mh.put_csv(client, PROCESSED_BUCKET, f"{prefix}/{t}.csv", df) for t, df in frames.items()}
        log.info("Số dòng mỗi bảng: %s", rows)
        return {
            "date": extracted["date"], "prefix": prefix, "date_id": int(extracted["date"].replace("-", "")),
            "rows": rows, "raw_keys": extracted["keys"],
        }

    @task
    def cdc(result: dict) -> dict:
        """So trạng thái giá hôm nay với snapshot đã commit gần nhất trước hôm nay."""
        client = mh.client()
        fact = mh.get_csv(client, PROCESSED_BUCKET, f"{result['prefix']}/fact_price.csv", dtype=winmart_cdc.SNAPSHOT_DTYPE)
        base = cdc_store.base_dt(client, SOURCE_PREFIX, CDC_SCOPE, result["date"])
        prev = cdc_store.load_snapshot(client, SOURCE_PREFIX, CDC_SCOPE, base, dtype=winmart_cdc.SNAPSHOT_DTYPE)

        changes, snapshot, stats = winmart_cdc.compute(prev, fact, result["date_id"])
        log.info("CDC so với mốc %s: %s", base, stats)
        if stats["partial_fetch"]:
            log.warning("Hôm nay lấy được ít sản phẩm bất thường (coverage=%s) - không đánh dấu sản phẩm biến mất", stats["coverage"])

        mh.put_csv(client, PROCESSED_BUCKET, f"{result['prefix']}/price_changes.csv", changes)
        cdc_store.save_snapshot(client, SOURCE_PREFIX, CDC_SCOPE, result["date"], snapshot)
        return {**result, "cdc": {"base_dt": base, "stats": stats, "changes": len(changes), "snapshot_rows": len(snapshot)}}

    @task
    def validate(result: dict) -> None:
        client = mh.client()
        for key in result["raw_keys"]:
            client.stat_object(RAW_BUCKET, key)
        for table in schema.SNAPSHOT_TABLES:
            stat = client.stat_object(PROCESSED_BUCKET, f"{result['prefix']}/{table}.csv")
            if result["rows"][table] == 0:
                raise ValueError(f"Bảng {table} rỗng")
            log.info("OK %s: %d dòng, %d bytes", table, result["rows"][table], stat.size)
        client.stat_object(PROCESSED_BUCKET, f"{result['prefix']}/price_changes.csv")

    @task
    def load_clickhouse(result: dict) -> None:
        """Tạo bảng nếu chưa có, rồi để ClickHouse tự đọc CSV từ MinIO bằng hàm s3()."""
        ch.ensure_schema(schema.DB, schema.TABLES, schema.VIEWS)

        date_id = result["date_id"]
        ch.query(f"ALTER TABLE {schema.DB}.fact_price DROP PARTITION {date_id}")  # chạy lại cùng ngày không bị trùng
        for table in schema.SNAPSHOT_TABLES:
            ch.load_csv_from_s3(schema.DB, table, schema.TABLES[table][0], f"{result['prefix']}/{table}.csv")

        # Thay đổi của 1 ngày luôn được tính lại so với cùng 1 mốc -> xoá rồi ghi lại là idempotent
        ch.query(f"DELETE FROM {schema.DB}.price_changes WHERE change_date_id = {date_id}")
        if result["cdc"]["changes"]:
            ch.load_csv_from_s3(schema.DB, "price_changes", schema.TABLES["price_changes"][0], f"{result['prefix']}/price_changes.csv")

        checks = {
            "fact_price": (ch.count(schema.DB, "fact_price", f"date_id = {date_id}"), result["rows"]["fact_price"]),
            "price_changes": (ch.count(schema.DB, "price_changes", f"change_date_id = {date_id}"), result["cdc"]["changes"]),
        }
        for table, (loaded, expected) in checks.items():
            if loaded != expected:
                raise ValueError(f"{table}: ClickHouse có {loaded} dòng, CSV có {expected} dòng")
        log.info("✅ ClickHouse OK: %s", {t: v[0] for t, v in checks.items()})

    @task
    def commit_cdc(result: dict) -> None:
        cdc_store.commit(mh.client(), SOURCE_PREFIX, CDC_SCOPE, result["date"], {
            "committed_at": pendulum.now(TZ).isoformat(), "raw_keys": result["raw_keys"], **result["cdc"],
        })

    buckets = ensure_buckets()
    extracted = extract()
    buckets >> extracted
    changed = cdc(transform(extracted))
    loaded = load_clickhouse(changed)
    validate(changed) >> loaded >> commit_cdc(changed)


winmart_api_pipeline()
