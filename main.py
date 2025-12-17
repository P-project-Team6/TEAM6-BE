from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from datetime import date as dt_date
import os

app = FastAPI(title="finsight API")

DB_URL = os.getenv(
    "DB_URL",
    "mysql+pymysql://root:1047@127.0.0.1:3306/finsight?charset=utf8mb4"
)

engine = create_engine(DB_URL, pool_pre_ping=True)


# 공통 유틸
def clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))

def get_source_id(conn, code: str = "NAVER") -> int:
    row = conn.execute(
        text("SELECT id FROM sources WHERE code=:code LIMIT 1;"),
        {"code": code}
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"No source: {code}")
    return int(row["id"])

@app.get("/")
def health():
    return {"ok": True, "service": "finsight"}


# Recommendations
@app.get("/recommendations/dates")
def recommendation_dates():
    """
    날짜별로 추천 데이터가 몇 개 들어있는지 + 누락 개수
    """
    q = text("""
        SELECT
          r.signal_date,
          COUNT(*) AS loaded_cnt,
          (SELECT COUNT(*) FROM stocks) AS total_stocks,
          (SELECT COUNT(*) FROM stocks) - COUNT(*) AS missing_cnt
        FROM stock_daily_recommendations r
        WHERE r.source_id = (SELECT id FROM sources WHERE code='NAVER')
        GROUP BY r.signal_date
        ORDER BY r.signal_date DESC;
    """)
    with engine.begin() as conn:
        rows = conn.execute(q).mappings().all()
    return {"items": rows}

@app.get("/recommendations/latest")
def latest_recommendations(
    limit: int = Query(20, ge=1, le=200),
    complete_only: bool = Query(True)
):
    """
    최신 추천 Top N
    - complete_only=True면 '종목이 전부 있는 날짜' 중 최신을 사용
    - complete_only=False면 그냥 MAX(signal_date) 사용
    """
    limit = clamp_int(limit, 1, 200)

    if complete_only:
        date_q = text("""
            SELECT r.signal_date
            FROM stock_daily_recommendations r
            WHERE r.source_id = (SELECT id FROM sources WHERE code='NAVER')
            GROUP BY r.signal_date
            HAVING COUNT(*) = (SELECT COUNT(*) FROM stocks)
            ORDER BY r.signal_date DESC
            LIMIT 1;
        """)
    else:
        date_q = text("""
            SELECT MAX(signal_date) AS signal_date
            FROM stock_daily_recommendations
            WHERE source_id = (SELECT id FROM sources WHERE code='NAVER');
        """)

    with engine.begin() as conn:
        d = conn.execute(date_q).mappings().first()
        if not d or not d["signal_date"]:
            raise HTTPException(status_code=404, detail="No recommendation data")

        chosen_date = d["signal_date"]

        rec_q = text("""
            SELECT
              r.stock_id, r.source_id, r.signal_date,
              r.positive_ratio, r.threshold_used, r.is_recommended,
              r.actual_is_up, r.is_hit,
              s.ticker AS stock_ticker,
              s.name_ko AS stock_name_ko,
              s.name_en AS stock_name_en
            FROM stock_daily_recommendations r
            JOIN stocks s ON s.id = r.stock_id
            WHERE r.source_id = (SELECT id FROM sources WHERE code='NAVER')
              AND r.signal_date = :signal_date
            ORDER BY r.positive_ratio DESC
            LIMIT :limit;
        """)

        items = conn.execute(
            rec_q,
            {"signal_date": chosen_date, "limit": limit}
        ).mappings().all()

    return {"signal_date": str(chosen_date), "items": items}

@app.get("/stocks/{stock_id}/recommendations")
def stock_recommendations(
    stock_id: int,
    limit: int = Query(60, ge=1, le=500)
):
    """
    종목별 추천 히스토리
    """
    limit = clamp_int(limit, 1, 500)

    q = text("""
        SELECT
          r.signal_date, r.positive_ratio, r.threshold_used,
          r.is_recommended, r.actual_is_up, r.is_hit
        FROM stock_daily_recommendations r
        WHERE r.stock_id = :stock_id
          AND r.source_id = (SELECT id FROM sources WHERE code='NAVER')
        ORDER BY r.signal_date DESC
        LIMIT :limit;
    """)

    with engine.begin() as conn:
        rows = conn.execute(q, {"stock_id": stock_id, "limit": limit}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="No data for this stock_id")

    return {"stock_id": stock_id, "items": rows}

# Hot Topics
@app.get("/hot-topics/latest")
def hot_topics_latest(
    limit: int = Query(20, ge=1, le=200),
    source_code: str = Query("NAVER")
):
    """
    최신 hot_topics Top N (CSV 결과 그대로)
    - 정렬: popularity DESC
    - growth 값이 음수여도 그대로 내려줌
    """
    limit = clamp_int(limit, 1, 200)

    with engine.begin() as conn:
        sid = get_source_id(conn, source_code)

        last = conn.execute(
            text("SELECT MAX(topic_date) AS d FROM hot_topics WHERE source_id=:sid;"),
            {"sid": sid}
        ).mappings().first()

        if not last or not last["d"]:
            raise HTTPException(status_code=404, detail="No hot_topics data")

        d = last["d"]

        q = text(f"""
            SELECT
              h.topic_date,
              s.id AS stock_id,
              s.ticker AS code,
              s.name_ko,
              h.mentions,
              h.mentions_7d_ma,
              h.daily_growth_pct,
              h.weekly_growth_pct,
              h.popularity
            FROM hot_topics h
            JOIN stocks s ON s.id = h.stock_id
            WHERE h.source_id = :sid
              AND h.topic_date = :d
            ORDER BY h.popularity DESC
            LIMIT {limit};
        """)

        rows = conn.execute(q, {"sid": sid, "d": d}).mappings().all()

    return {"topic_date": str(d), "items": rows}

@app.get("/hot-topics")
def hot_topics_by_date(
    date_: dt_date = Query(..., alias="date"),
    limit: int = Query(50, ge=1, le=500),
    source_code: str = Query("NAVER")
):
    """
    특정 날짜 hot_topics Top N (CSV 결과 그대로)
    예) /hot-topics?date=2025-12-05&limit=20
    """
    limit = clamp_int(limit, 1, 500)

    with engine.begin() as conn:
        sid = get_source_id(conn, source_code)

        q = text(f"""
            SELECT
              h.topic_date,
              s.id AS stock_id,
              s.ticker AS code,
              s.name_ko,
              h.mentions,
              h.mentions_7d_ma,
              h.daily_growth_pct,
              h.weekly_growth_pct,
              h.popularity
            FROM hot_topics h
            JOIN stocks s ON s.id = h.stock_id
            WHERE h.source_id = :sid
              AND h.topic_date = :d
            ORDER BY h.popularity DESC
            LIMIT {limit};
        """)

        rows = conn.execute(q, {"sid": sid, "d": date_}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="No hot_topics data for this date")

    return {"topic_date": str(date_), "items": rows}
