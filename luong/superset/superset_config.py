"""Cấu hình Superset cho đồ án (nạp qua PYTHONPATH=/app/pythonpath trong image chính thức)."""
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "int1422-kho-du-lieu-nong-san-doi-khi-trien-khai-that")
# Metadata của Superset (người dùng, biểu đồ, dashboard) - không phải dữ liệu kho; SQLite là đủ cho 1 máy
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,   # bộ lọc dashboard = cắt lát / cắt khối
    "DASHBOARD_CROSS_FILTERS": True,    # bấm vào 1 cột để lọc các biểu đồ khác
    "DRILL_BY": True,                   # chuột phải > Drill by = khoan xuống theo chiều khác
    "DRILL_TO_DETAIL": True,            # chuột phải > Drill to detail = xem dòng chi tiết
    "ENABLE_TEMPLATE_PROCESSING": True,
}
ROW_LIMIT = 50000
SQLLAB_TIMEOUT = 120
