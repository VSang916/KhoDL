"""Code dùng chung cho các DAG. Không chứa DAG nào (đã khai báo trong dags/.airflowignore).

pipelines/common   : MinIO, ClickHouse, HTTP session - dùng cho mọi nguồn
pipelines/winmart  : gọi API Winmart + biến đổi thành bảng
pipelines/ttns     : cào thitruongnongsan.gov.vn + làm sạch

Các module ở đây không import airflow nên test được bằng pytest thuần (xem luong/tests).
"""
