# Kho dữ liệu & khai phá giá nông sản Việt Nam

Bài tập lớn **INT1422 — Kho dữ liệu và Khai phá dữ liệu**. Hệ thống thu thập giá rau quả từ hai nguồn công khai, nạp vào
kho dữ liệu theo lược đồ hình sao, cung cấp dịch vụ OLAP và các chức năng khai phá (dự báo giá, phân cụm, phát hiện dị
biệt) qua một ứng dụng web.

## Câu hỏi nghiệp vụ
1. Giá rau quả biến động thế nào theo thời gian; năm nay đắt hay rẻ hơn năm trước? — *người tiêu dùng, cơ quan quản lý giá*
2. Mặt hàng nào tăng giá mạnh nhất, giá tháng tới dự kiến ra sao? — *doanh nghiệp bán lẻ*
3. Giá tới tay người mua gấp bao nhiêu lần giá thu mua tại vườn? — *nông dân, hợp tác xã*
4. Mặt hàng nào có cùng nhịp giá theo mùa; chuỗi giá nào bất thường? — *thương lái, đơn vị thu mua*

## Kiến trúc
```
Nguồn                    Thu nạp (Airflow)              Data Lake (MinIO)         Kho dữ liệu (ClickHouse)        Khai thác
thitruongnongsan.gov.vn ─► scraper ASP.NET ─┐                                    ┌► fact_price_monthly + 4 chiều ─► OLAP (5 thao tác, 3 khối con) ─► Superset
  (giá tháng 2021-2025)                     ├► raw-data/ (file gốc) ─► làm sạch ─┤   price_clean, cdc_log          ─► ứng dụng Streamlit
Winmart API ───────────► API connector ─────┘  processed-data/ + snapshot CDC   └► winmart.* (giá ngày, SCD2)     ─► dự báo / phân cụm / dị biệt (ttns.ml_*)
  (giá siêu thị hằng ngày)
```
Sơ đồ chi tiết: `luong_du_lieu_nong_san_chi_tiet.drawio` (3 trang: tổng quan, luồng Winmart, luồng TTNS kèm CDC).

## Chạy lại từ đầu bằng một lệnh
Yêu cầu: Docker Desktop, Python 3.10+ (nên dùng virtualenv), Git Bash nếu dùng Windows.
```bash
cd luong
./run_all.sh            # Docker -> ETL + CDC + khối OLAP -> ML -> Superset -> notebook -> kiểm thử -> ứng dụng http://localhost:8501
```
Giao diện quản trị: Airflow http://localhost:8082 (airflow/airflow) · MinIO http://localhost:9001 (minioadmin/minioadmin) ·
ClickHouse http://localhost:8123 (default/mypassword) · **Superset http://localhost:8088 (admin/admin)**, dashboard
*Kho giá nông sản — OLAP* và *Kho giá nông sản — Khai phá*.

## Sản phẩm theo tiêu chí chấm (Bài 12)
| Phần | Tiêu chí | Ở đâu |
|---|---|---|
| 1. Thu thập & tiền xử lý | nguồn, số bản ghi, thuộc tính, thời gian; tỷ lệ thiếu và cách xử lý từng cột | `luong/olap/OLAP_GiaNongSan.ipynb` mục 1 · `luong/dags/pipelines/ttns/cleaning.py` |
| 2. Thiết kế kho (CLO1.TC1) | lược đồ hình sao, 4 chiều đều có phân cấp, độ mịn và lý do | notebook OLAP mục 2 · `luong/dags/pipelines/ttns/schema.py` |
| 3. Cài đặt & tính khối (CLO1.TC2) | ETL nạp kho; 3 khối con chọn bằng thuật toán tham lam, đo lợi ích bằng `query_log` | DAG `luong/dags/ttns_price_pipeline.py` · `luong/dags/pipelines/ttns/olap.py` · notebook OLAP mục 3 |
| 4. Dịch vụ OLAP | cuộn lên, khoan xuống, cắt lát, cắt khối, xoay trục, `CUBE`/`ROLLUP` trên dữ liệu thật | notebook OLAP mục 4 · tab **OLAP** của ứng dụng · dashboard Superset (mỗi biểu đồ đọc khối con nhỏ nhất trả lời được) |
| 5. Khai phá (CLO2.TC1) | hồi quy (LR, Decision Tree, Random Forest, 2 mô hình cơ sở, CV theo thời gian), K-Means (Elbow + Silhouette), DBSCAN | `luong/ml/DeThucHanh01_GiaNongSan.ipynb` |
| Ứng dụng khai phá (CLO2.TC2) | ứng dụng web: OLAP tương tác, dự báo giá kèm khoảng 80%, phân cụm & dị biệt; kết quả khai phá lưu trong kho | `luong/app/app.py` · `luong/ml/publish_ml.py` · `luong/superset/` |
| Chất lượng mã | kiểm thử tự động (15 test), CDC, chạy lại một lệnh | `luong/tests/` · `luong/run_all.sh` |

## Cấu trúc thư mục
```
luong/
  docker-compose.yml, Dockerfile     hạ tầng: Airflow (Celery), MinIO, ClickHouse, Postgres, Redis
  run_all.sh                         chạy lại toàn bộ
  dags/                              2 DAG + package pipelines/ (common, winmart, ttns: scraper, cleaning, cdc, schema, olap)
  olap/OLAP_GiaNongSan.ipynb         thiết kế kho, chọn khối con, 5 thao tác OLAP, trả lời câu hỏi nghiệp vụ
  ml/DeThucHanh01_GiaNongSan.ipynb   khai phá: EDA, tiền xử lý, hồi quy, phân cụm (theo cấu trúc Đề thực hành 01)
  ml/phan_tich_gia_nong_san.ipynb    phân tích chuỗi thời gian mở rộng (dự báo nhiều bước, khoảng giá, mùa vụ)
  ml/publish_ml.py                   ghi kết quả khai phá vào kho
  app/app.py                         ứng dụng Streamlit
  superset/                          Superset (Docker) + bootstrap.py dựng sẵn dataset, biểu đồ, dashboard
  tests/                             pytest
  README                             tài liệu kỹ thuật chi tiết (luồng, CDC, từ điển dữ liệu)
data/ttns/                           mẫu file xuất từ nguồn + script cào độc lập
Tai_lieu_tham_khao/VI/               slide bài giảng
```

## Dữ liệu và giấy phép
- **thitruongnongsan.gov.vn** — cổng thông tin thị trường nông sản (nhà nước), dữ liệu công khai; dùng cho mục đích học tập.
  40.575 lần báo giá, 11 thuộc tính, 04/2021 → 12/2025, có thuộc tính thời gian (tháng). Không chứa dữ liệu cá nhân.
- **Winmart** — giá niêm yết công khai trên website bán lẻ, thu bằng API của trang; chỉ dùng minh hoạ luồng hằng ngày và CDC
  (mới tích luỹ vài ngày, chưa đủ cho khai phá).

## Khai báo sử dụng công cụ AI
Nhóm sử dụng Claude (Anthropic) hỗ trợ viết mã, thiết kế luồng ETL/CDC, notebook phân tích và tài liệu. Nhóm chịu trách
nhiệm kiểm tra, chạy lại và giải thích toàn bộ kết quả.
