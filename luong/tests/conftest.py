import sys
from pathlib import Path

# Giống Airflow: thư mục dags/ nằm trong sys.path nên import được package pipelines
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
