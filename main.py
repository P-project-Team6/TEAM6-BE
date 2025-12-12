from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os

app = FastAPI(title="finsight API")

DB_URL = os.getenv(
    "DB_URL",
    "mysql+pymysql://root:1047@127.0.0.1:3306/finsight?charset=utf8mb4"
)


engine = create_engine(DB_URL, pool_pre_ping=True)

@app.get("/")
def health():
    return {"ok": True, "service": "finsight"}

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
def latest_recommendations(limit: int = 20, complete_only: bool = True):
    """
    최신 추천 Top N
    - complete_only=True면 '종목 79개가 다 있는 날짜' 중 최신을 사용
    - complete_only=False면 그냥 MAX(signal_date) 사용
    """
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

        items = conn.execute(rec_q, {"signal_date": chosen_date, "limit": limit}).mappings().all()

    return {"signal_date": str(chosen_date), "items": items}

@app.get("/stocks/{stock_id}/recommendations")
def stock_recommendations(stock_id: int, limit: int = 60):
    """
    종목별 추천 히스토리
    """
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
