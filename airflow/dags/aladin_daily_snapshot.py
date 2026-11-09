from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

LOOKUP_DIR = Path("/opt/airflow/data/raw/lookup")


@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 2),
    end_date=datetime(2026, 10, 30),
    catchup=False,          # 오늘은 끄고, 수요일 백필 때 켤 거예요
    tags=["aladin"],
)
def aladin_daily_snapshot():

    @task
    def check_source(ds=None):
        """실행 날짜(ds)에 해당하는 수집 파일이 있는지 확인하고 건수를 센다."""
        path = LOOKUP_DIR / f"date={ds}" / "lookup.jsonl"
        if not path.exists():
            raise FileNotFoundError(f"{ds} 수집 파일 없음: {path}")
        n = sum(1 for _ in path.open(encoding="utf-8"))
        print(f"{ds}: {n}건")
        return n

    check_source()


aladin_daily_snapshot()