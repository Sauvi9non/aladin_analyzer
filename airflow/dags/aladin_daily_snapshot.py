"""
알라딘 일일 스냅샷 파이프라인

GitHub Actions로 매일 수집한 알라딘 ItemLookUp 결과(data/raw/lookup/date=YYYY-MM-DD/)를
실행 날짜(ds) 단위로 검증하고 DuckDB에 적재한다.

check_source → validate → load_raw
"""
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException

LOOKUP_DIR = Path("/opt/airflow/data/raw/lookup")
WAREHOUSE = Path("/opt/airflow/warehouse")
STAGING_DIR = WAREHOUSE / "staging"
DB_PATH = WAREHOUSE / "aladin.duckdb"

MIN_ROWS = 2000  # GitHub Actions 검증 단계와 같은 기준


# ---------------------------------------------------------------
# 도우미 함수 (Airflow 없이도 테스트할 수 있게 태스크 밖에 둠)
# ---------------------------------------------------------------
def _used(sub: dict, channel: str, field: str):
    """usedList의 채널별 값. 없으면 0."""
    return ((sub.get("usedList") or {}).get(channel) or {}).get(field, 0)


def flatten(record: dict, snapshot_date: str) -> dict:
    """ItemLookUp 응답 1건을 분석용 컬럼으로 평탄화."""
    sub = record.get("subInfo") or {}
    rating = sub.get("ratingInfo") or {}
    return {
        "snapshot_date": snapshot_date,
        "isbn13": str(record.get("_isbn13") or record.get("isbn13") or ""),
        "item_id": record.get("itemId"),
        "title": record.get("title"),
        "mall_type": record.get("mallType"),
        "category_name": record.get("categoryName"),
        "publisher": record.get("publisher"),
        "price_sales": record.get("priceSales"),
        "price_standard": record.get("priceStandard"),
        "sales_point": record.get("salesPoint"),
        "stock_status": record.get("stockStatus"),
        "customer_review_rank": record.get("customerReviewRank"),
        "used_aladin_cnt": _used(sub, "aladinUsed", "itemCount"),   # 알라딘 직매입 (C2B)
        "used_aladin_min": _used(sub, "aladinUsed", "minPrice"),
        "used_user_cnt": _used(sub, "userUsed", "itemCount"),       # 회원 판매 (C2C)
        "used_user_min": _used(sub, "userUsed", "minPrice"),
        "used_space_cnt": _used(sub, "spaceUsed", "itemCount"),     # 오프라인 매장
        "used_space_min": _used(sub, "spaceUsed", "minPrice"),
        "rating_score": rating.get("ratingScore"),
        "rating_count": rating.get("ratingCount"),
        "collected_at": record.get("_collected_at"),
    }


def clean(rows: list[dict]) -> tuple[pd.DataFrame, dict]:
    """품질 규칙을 적용하고 (정제된 데이터, 품질 통계)를 돌려준다."""
    df = pd.DataFrame(rows)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    raw_rows = len(df)

    invalid = df["isbn13"].isin(["", "0"])           # 잘못된 키: 엉뚱한 책이 조회됨
    df = df[~invalid]
    dup = df.duplicated(subset="isbn13", keep="first")  # 같은 날 같은 책 중복
    df = df[~dup]

    stats = {
        "raw_rows": raw_rows,
        "dropped_invalid_isbn": int(invalid.sum()),
        "dropped_duplicate": int(dup.sum()),
        "clean_rows": len(df),
    }
    return df, stats


def load(df: pd.DataFrame, stats: dict, snapshot_date: str, db_path: Path) -> None:
    """날짜 단위로 지우고 다시 넣어, 몇 번을 다시 돌려도 결과가 같게(멱등) 적재."""
    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_snapshot AS
            SELECT * FROM df WHERE 1 = 0
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_log (
                snapshot_date DATE, raw_rows INT, dropped_invalid_isbn INT,
                dropped_duplicate INT, clean_rows INT, loaded_at TIMESTAMP
            )
        """)
        con.execute("BEGIN")
        con.execute("DELETE FROM raw_snapshot WHERE snapshot_date = ?", [snapshot_date])
        con.execute("INSERT INTO raw_snapshot SELECT * FROM df")
        con.execute("DELETE FROM dq_log WHERE snapshot_date = ?", [snapshot_date])
        con.execute(
            "INSERT INTO dq_log VALUES (?, ?, ?, ?, ?, now())",
            [snapshot_date, stats["raw_rows"], stats["dropped_invalid_isbn"],
             stats["dropped_duplicate"], stats["clean_rows"]],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


# ---------------------------------------------------------------
# DAG
# ---------------------------------------------------------------
@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 2),
    end_date=datetime(2026, 10, 30),
    catchup=True,          
    max_active_runs=1,      # DuckDB 파일은 한 번에 하나만 쓰도록
    tags=["aladin"],
)
def aladin_daily_snapshot():

    @task
    def check_source(ds=None) -> str:
        """그날 수집 파일이 없거나 비어 있으면 실패가 아니라 '건너뛰기'."""
        path = LOOKUP_DIR / f"date={ds}" / "lookup.jsonl"
        if not path.exists() or path.stat().st_size == 0:
            raise AirflowSkipException(f"{ds}: 수집 데이터 없음 → 건너뜀")
        return str(path)

    @task
    def validate(path: str, ds=None) -> dict:
        """평탄화 + 품질 규칙 적용 후 스테이징 파일로 저장."""
        with open(path, encoding="utf-8") as f:
            rows = [flatten(json.loads(line), ds) for line in f]
        df, stats = clean(rows)
        print(f"{ds} 품질 체크: {stats}")

        if stats["clean_rows"] < MIN_ROWS:
            raise AirflowFailException(
                f"{ds}: 정제 후 {stats['clean_rows']}건 (기준 {MIN_ROWS}건 미달)"
            )

        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staging = STAGING_DIR / f"{ds}.parquet"
        duckdb.sql("SELECT * FROM df").write_parquet(str(staging))
        return {"staging": str(staging), **stats}

    @task
    def load_raw(result: dict, ds=None) -> None:
        df = duckdb.read_parquet(result["staging"]).df()
        load(df, result, ds, DB_PATH)
        print(f"{ds}: {result['clean_rows']}건 적재 완료")

    load_raw(validate(check_source()))


aladin_daily_snapshot()