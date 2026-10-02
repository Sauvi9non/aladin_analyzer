#!/usr/bin/env python3
"""
알라딘 OpenAPI 수집기 — 2026-10-30 서비스 종료 대응

모드
  backfill : 주차별 베스트셀러 히스토리 일괄 수집 (1회성, 오늘 중 완료)
  panel    : backfill 결과에서 매일 추적할 ISBN 패널 확정 (1회성)
  snapshot : 패널 전체에 ItemLookUp 실행 (매일 1회, 10/30까지)

사용 예
  export TTB_KEY='ttbxxxdmstj1046001'
  python aladin_collect.py backfill --start 2021-01 --end 2026-10
  python aladin_collect.py panel --limit 3000
  python aladin_collect.py snapshot
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

API_BASE = "https://www.aladin.co.kr/ttb/api"
VERSION = "20131101"

DATA = Path("data")
RAW_BEST = DATA / "raw" / "bestseller"
RAW_LOOKUP = DATA / "raw" / "lookup"
LOG_PATH = DATA / "logs" / "collection_log.csv"
PANEL_PATH = DATA / "panel.csv"

# 일일 호출 한도. 알라딘 마이페이지에서 실제 한도 확인 후 조정할 것.
DAILY_BUDGET = int(os.environ.get("ALADIN_DAILY_BUDGET", "4500"))
SLEEP = float(os.environ.get("ALADIN_SLEEP", "0.25"))

_calls = 0


class BudgetExceeded(Exception):
    pass


def key() -> str:
    k = os.environ.get("TTB_KEY")
    if not k:
        sys.exit("TTB_KEY 환경변수가 없습니다. export TTB_KEY='ttb...'")
    return k


def parse_loose(text: str):
    """알라딘 응답은 제어문자 때문에 json.loads가 깨질 때가 있어 보정."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        cleaned = cleaned.rstrip().rstrip(";")
        return json.loads(cleaned)


def call(endpoint: str, params: dict, retries: int = 4):
    """API 1회 호출. 예산 초과 시 BudgetExceeded."""
    global _calls
    if _calls >= DAILY_BUDGET:
        raise BudgetExceeded(f"일일 예산 {DAILY_BUDGET}회 소진")

    params = {**params, "ttbkey": key(), "output": "js", "Version": VERSION}
    url = f"{API_BASE}/{endpoint}?{urlencode(params)}"

    for attempt in range(retries):
        try:
            _calls += 1
            with urlopen(url, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = parse_loose(raw)
            if isinstance(data, dict) and data.get("errorCode"):
                # 한도 초과 등 복구 불가 에러는 즉시 중단
                msg = f"{data.get('errorCode')}: {data.get('errorMessage')}"
                if "초과" in str(data.get("errorMessage", "")):
                    raise BudgetExceeded(msg)
                print(f"  [api error] {msg}", file=sys.stderr)
                return None
            time.sleep(SLEEP)
            return data
        except BudgetExceeded:
            raise
        except (URLError, HTTPError, json.JSONDecodeError, TimeoutError) as e:
            wait = (2 ** attempt) + random.random()
            print(f"  [retry {attempt+1}/{retries}] {type(e).__name__} {e} — {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
    return None


def log(mode: str, target: str, ok: int, fail: int, note: str = ""):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    new = not LOG_PATH.exists()
    with LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["run_at", "mode", "target", "ok", "fail", "api_calls", "note"])
        w.writerow([datetime.now(timezone.utc).isoformat(), mode, target, ok, fail, _calls, note])


def months(start: str, end: str):
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            y, m = y + 1, 1


# ---------------------------------------------------------------- backfill

def backfill(args):
    """주차별 베스트셀러 히스토리. 과거 소급이 되는 신간/전자책만 의미 있음."""
    RAW_BEST.mkdir(parents=True, exist_ok=True)
    targets = args.targets.split(",")
    ok = fail = 0

    for target in targets:
        out = RAW_BEST / f"{target.lower()}_weekly.jsonl"
        done = set()
        if out.exists():  # 재실행 시 이어하기
            with out.open(encoding="utf-8") as f:
                for line in f:
                    try:
                        r = json.loads(line)
                        done.add((r["_year"], r["_month"], r["_week"], r["_page"]))
                    except Exception:
                        pass
            print(f"[{target}] 기존 {len(done)}개 주차/페이지 건너뜀")

        with out.open("a", encoding="utf-8") as f:
            for y, m in months(args.start, args.end):
                for week in range(1, 6):
                    for page in range(1, args.pages + 1):
                        if (y, m, week, page) in done:
                            continue
                        try:
                            data = call("ItemList.aspx", {
                                "QueryType": "Bestseller",
                                "SearchTarget": target,
                                "Year": y, "Month": m, "Week": week,
                                "MaxResults": 50, "start": page,
                            })
                        except BudgetExceeded as e:
                            print(f"\n중단: {e}\n내일 같은 명령 다시 실행하면 이어서 진행됩니다.")
                            log("backfill", target, ok, fail, str(e))
                            return
                        if not data or not data.get("item"):
                            fail += 1
                            continue
                        for it in data["item"]:
                            it["_year"], it["_month"], it["_week"], it["_page"] = y, m, week, page
                            it["_target"] = target
                            f.write(json.dumps(it, ensure_ascii=False) + "\n")
                        ok += 1
                        print(f"[{target}] {y}-{m:02d} W{week} p{page} → {len(data['item'])}건 (누적 {_calls}콜)")
                f.flush()
    log("backfill", args.targets, ok, fail)
    print(f"\n완료: 성공 {ok} / 빈응답 {fail} / 호출 {_calls}")


# ------------------------------------------------------------------- panel

def panel(args):
    """추적 대상 ISBN 확정. 최근 등장 + 등장 빈도 순으로 상위 N권."""
    seen = {}
    for path in RAW_BEST.glob("*_weekly.jsonl"):
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                isbn = r.get("isbn13") or r.get("isbn")
                if not isbn:
                    continue
                ym = (r["_year"], r["_month"])
                cur = seen.setdefault(isbn, {
                    "isbn13": isbn, "title": r.get("title", ""),
                    "author": r.get("author", ""), "publisher": r.get("publisher", ""),
                    "category_id": r.get("categoryId", ""), "pub_date": r.get("pubDate", ""),
                    "appearances": 0, "last_ym": ym, "best_rank": r.get("bestRank") or 999,
                })
                cur["appearances"] += 1
                cur["last_ym"] = max(cur["last_ym"], ym)
                if r.get("bestRank"):
                    cur["best_rank"] = min(cur["best_rank"], r["bestRank"])

    rows = sorted(seen.values(), key=lambda r: (r["last_ym"], r["appearances"]), reverse=True)

    if args.stratify:
        # 출간연도 구간별 쿼터 — 전환 단계가 고르게 섞이도록
        quota = {"2026": 1000, "2025": 800, "2023-2024": 700, "2021-2022": 500}
        def bucket(r):
            y = (r.get("pub_date") or "")[:4]
            if not y.isdigit():
                return None
            y = int(y)
            if y >= 2026: return "2026"
            if y == 2025: return "2025"
            if y >= 2023: return "2023-2024"
            if y >= 2021: return "2021-2022"
            return None
        picked, used = [], Counter()
        for r in rows:
            b = bucket(r)
            if b and used[b] < quota[b]:
                picked.append(r); used[b] += 1
        rows = picked
        print("구간별 선정:", dict(used))
    elif args.limit:
        rows = rows[:args.limit]

    PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with PANEL_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "isbn13", "title", "author", "publisher", "category_id",
            "pub_date", "appearances", "last_ym", "best_rank"])
        w.writeheader()
        for r in rows:
            r["last_ym"] = f"{r['last_ym'][0]}-{r['last_ym'][1]:02d}"
            w.writerow(r)

    years = Counter(r["last_ym"][:4] for r in rows)
    print(f"패널 {len(rows)}권 → {PANEL_PATH}")
    print("최근 등장 연도 분포:", dict(sorted(years.items())))


# ---------------------------------------------------------------- snapshot

OPT = "usedList,ebookList,ratingInfo,c2bsales,bestSellerRank,packing"


def snapshot(args):
    """패널 전체 ItemLookUp. 하루 1회. 중단돼도 같은 날 재실행하면 이어감."""
    if not PANEL_PATH.exists():
        sys.exit("패널이 없습니다. 먼저 panel 모드를 실행하세요.")

    run_date = args.date or date.today().isoformat()
    outdir = RAW_LOOKUP / f"date={run_date}"
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / "lookup.jsonl"

    done = set()
    if out.exists():
        with out.open(encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["_isbn13"])
                except Exception:
                    pass
        print(f"오늘 이미 수집한 {len(done)}권 건너뜀")

    with PANEL_PATH.open(encoding="utf-8") as f:
        isbns = [r["isbn13"] for r in csv.DictReader(f)]
    todo = [i for i in isbns if i not in done]
    print(f"대상 {len(todo)}권 / 전체 패널 {len(isbns)}권")

    ok = fail = 0
    with out.open("a", encoding="utf-8") as f:
        for n, isbn in enumerate(todo, 1):
            try:
                data = call("ItemLookUp.aspx", {
                    "itemIdType": "ISBN13", "ItemId": isbn, "OptResult": OPT,
                })
            except BudgetExceeded as e:
                print(f"\n중단: {e} — 같은 날 재실행하면 이어서 진행됩니다.")
                log("snapshot", run_date, ok, fail, str(e))
                return
            if not data or not data.get("item"):
                fail += 1
                continue
            rec = data["item"][0]
            rec["_isbn13"] = isbn
            rec["_collected_at"] = datetime.now(timezone.utc).isoformat()
            rec["_run_date"] = run_date
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            ok += 1
            if n % 200 == 0:
                f.flush()
                print(f"  {n}/{len(todo)} (성공 {ok} 실패 {fail}, 호출 {_calls})")

    log("snapshot", run_date, ok, fail)
    print(f"\n{run_date} 완료: 성공 {ok} / 실패 {fail} / 호출 {_calls}")
    if fail > len(todo) * 0.1:
        print("경고: 실패율 10% 초과 — 응답을 직접 확인하세요.", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description="알라딘 OpenAPI 수집기")
    sub = p.add_subparsers(dest="mode", required=True)

    b = sub.add_parser("backfill", help="주차별 베스트셀러 히스토리 (1회성)")
    b.add_argument("--start", default="2021-01")
    b.add_argument("--end", default=date.today().strftime("%Y-%m"))
    b.add_argument("--targets", default="Book,eBook")
    b.add_argument("--pages", type=int, default=1, help="주차당 페이지 수 (1=50위까지)")
    b.set_defaults(func=backfill)

    pa = sub.add_parser("panel", help="추적 ISBN 패널 생성 (1회성)")
    pa.add_argument("--limit", type=int, default=3000, help="일일 한도 안에 들어오는 권수")
    pa.add_argument("--stratify", action="store_true", help="출간연도 구간별 할당")
    pa.set_defaults(func=panel)

    s = sub.add_parser("snapshot", help="일별 ItemLookUp (매일)")
    s.add_argument("--date", default=None)
    s.set_defaults(func=snapshot)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
