#!/usr/bin/env bash
# Chạy lại TOÀN BỘ dự án từ đầu bằng một lệnh (yêu cầu bắt buộc - Bài 12):
#   hạ tầng (Docker) -> ETL + CDC + khối OLAP (Airflow) -> khai phá (ML) -> dashboard Superset -> notebook -> kiểm thử -> ứng dụng
#
# Cách dùng (Git Bash / Linux / macOS, trong thư mục luong/):
#   ./run_all.sh                 # chạy hết, cuối cùng mở ứng dụng ở http://localhost:8501
#   NO_APP=1 ./run_all.sh        # không mở ứng dụng
#   SKIP_WINMART=1 ./run_all.sh  # bỏ qua nguồn Winmart (gọi API thật)
#   SKIP_NOTEBOOKS=1 ./run_all.sh
# Yêu cầu: Docker Desktop đang chạy; Python 3.10+ (nên dùng virtualenv).
set -euo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1   # Git Bash trên Windows: không tự đổi đường dẫn /opt/... trong lệnh docker

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
wait_healthy() {   # $1 = tên container, $2 = số giây tối đa
  local name=$1 limit=${2:-300} t=0 state
  until state=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$name" 2>/dev/null) \
        && [[ $state == healthy || $state == running && $name == clickhouse ]]; do
    (( t >= limit )) && { echo "Hết thời gian chờ $name (trạng thái: ${state:-không có})"; exit 1; }
    sleep 5; t=$((t + 5))
  done
  echo "$name: ${state}"
}

step "1/7 Khởi động hạ tầng: Airflow, MinIO, ClickHouse, Postgres, Redis, Superset"
docker compose up -d --build
wait_healthy clickhouse 180
wait_healthy luong-minio-1 180
wait_healthy luong-airflow-scheduler-1 600
SCHED=luong-airflow-scheduler-1

step "2/7 Kiểm tra DAG nạp được"
docker exec "$SCHED" airflow dags reserialize >/dev/null 2>&1 || true
docker exec "$SCHED" airflow dags list-import-errors 2>/dev/null | grep -q "No data found" \
  || { docker exec "$SCHED" airflow dags list-import-errors; exit 1; }

step "3/7 ETL nguồn thitruongnongsan.gov.vn: Extract -> MinIO -> làm sạch -> CDC -> ClickHouse -> khối OLAP"
docker exec "$SCHED" airflow dags test ttns_price_pipeline -c '{"force": true}' 2>&1 \
  | grep -E "CDC scope|ClickHouse OK|Khối con OLAP|ERROR - [^C]|DagRun Finished" | sed -E 's/^\[[^]]*\] \{[^}]*\} //'
if [[ -z ${SKIP_WINMART:-} ]]; then
  step "    ETL nguồn Winmart API (giá siêu thị hằng ngày, CDC trạng thái giá)"
  docker exec "$SCHED" airflow dags test winmart_api_pipeline 2>&1 \
    | grep -E "CDC so|ClickHouse OK|ERROR - [^C]|DagRun Finished" | sed -E 's/^\[[^]]*\] \{[^}]*\} //'
fi

step "4/7 Cài thư viện phân tích trên máy"
python -m pip install -q -r requirements-analysis.txt

step "5/7 Khai phá: huấn luyện mô hình cuối, ghi dự báo / cụm / dị biệt vào kho (ttns.ml_*)"
python ml/publish_ml.py

step "    Superset: kết nối kho, dataset trên 3 khối con, dashboard OLAP + khai phá"
wait_healthy luong-superset-1 300
docker compose exec -T superset python /app/bootstrap.py

if [[ -z ${SKIP_NOTEBOOKS:-} ]]; then
  step "6/7 Chạy lại các notebook báo cáo"
  for nb in olap/OLAP_GiaNongSan ml/DeThucHanh01_GiaNongSan; do
    ( cd "$(dirname "$nb")" && jupytext --to notebook "$(basename "$nb").py" >/dev/null \
      && jupyter nbconvert --execute --to notebook --inplace --ExecutePreprocessor.timeout=3600 "$(basename "$nb").ipynb" \
      && echo "OK $nb.ipynb" )
  done
fi

step "7/7 Kiểm thử"
python -m pytest tests -q -p no:cacheprovider

if [[ -z ${NO_APP:-} ]]; then
  step "Mở ứng dụng: http://localhost:8501  (Ctrl+C để dừng)"
  python -m streamlit run app/app.py --server.headless true
fi
