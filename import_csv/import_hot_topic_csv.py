import os
import sys
import pandas as pd
import pymysql
from datetime import datetime

DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "1047"),
    "database": os.getenv("DB_NAME", "finsight"),
    "charset": "utf8mb4",
    "autocommit": False,
}

CSV_PATH = os.getenv("HOT_TOPIC_CSV", "top_increasing_stocks.csv")
SOURCE_CODE = os.getenv("HOT_TOPIC_SOURCE", "NAVER")

REQUIRED_COLS = [
    "Date", "Code", "mentions", "daily_growth", "weekly_growth", "popularity", "mentions_7d_ma"
]

def read_csv_safely(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    raise RuntimeError("CSV 인코딩을 읽지 못했습니다. (utf-8/utf-8-sig/cp949 모두 실패)")

def zfill6(code: str) -> str:
    return str(code).strip().zfill(6)

def parse_date_yyyy_mm_dd(s) -> str | None:
    s = str(s).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        return None

def main():
    path = CSV_PATH
    if len(sys.argv) >= 2:
        path = sys.argv[1]

    df = read_csv_safely(path)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV에 필요한 컬럼이 없습니다: {missing}")

    df["Code"] = df["Code"].astype(str).map(zfill6)
    df["Date"] = df["Date"].apply(parse_date_yyyy_mm_dd)
    df = df.dropna(subset=["Date"])

    df["mentions"] = pd.to_numeric(df["mentions"], errors="coerce").fillna(0).astype(int)
    df["mentions_7d_ma"] = pd.to_numeric(df["mentions_7d_ma"], errors="coerce").fillna(0.0).astype(float)
    df["daily_growth"] = pd.to_numeric(df["daily_growth"], errors="coerce").fillna(0.0).astype(float)
    df["weekly_growth"] = pd.to_numeric(df["weekly_growth"], errors="coerce").fillna(0.0).astype(float)
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce").fillna(0.0).astype(float)

    # CSV는 %로 저장
    df["daily_growth_pct"] = df["daily_growth"] * 100.0
    df["weekly_growth_pct"] = df["weekly_growth"] * 100.0

    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM sources WHERE code=%s LIMIT 1;", (SOURCE_CODE,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"sources 테이블에 code='{SOURCE_CODE}'가 없습니다.")
            source_id = int(row[0])

            cur.execute("SELECT id, ticker FROM stocks;")
            stock_map = {str(t).strip(): int(i) for (i, t) in cur.fetchall()}

            upsert_sql = """
            INSERT INTO hot_topics
              (source_id, topic_date, stock_id,
               mentions, mentions_7d_ma,
               daily_growth_pct, weekly_growth_pct,
               popularity)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              mentions = VALUES(mentions),
              mentions_7d_ma = VALUES(mentions_7d_ma),
              daily_growth_pct = VALUES(daily_growth_pct),
              weekly_growth_pct = VALUES(weekly_growth_pct),
              popularity = VALUES(popularity),
              updated_at = CURRENT_TIMESTAMP;
            """

            upserted = 0
            skipped_stock = 0

            for _, r in df.iterrows():
                stock_id = stock_map.get(r["Code"])
                if stock_id is None:
                    skipped_stock += 1
                    continue

                cur.execute(upsert_sql, (
                    source_id,
                    r["Date"],
                    stock_id,
                    int(r["mentions"]),
                    float(r["mentions_7d_ma"]),
                    float(r["daily_growth_pct"]),
                    float(r["weekly_growth_pct"]),
                    float(r["popularity"]),
                ))
                upserted += 1

        conn.commit()
        print(f"완료. upserted={upserted}, skipped_stock={skipped_stock}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()

