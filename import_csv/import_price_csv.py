import os
import csv
from decimal import Decimal, InvalidOperation
import pymysql

# DB 설정 (원하면 env로 교체 가능)
DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "1047"),
    "database": os.getenv("DB_NAME", "finsight"),
    "charset": "utf8mb4",
    "autocommit": False,
}

CSV_PATH = os.getenv("PRICE_CSV", "stock_price_data_top80.csv")

TIMEFRAME = os.getenv("TIMEFRAME", "1H")          # 1H / 1D 등
MARKET_TYPE = os.getenv("MARKET_TYPE", "DOMESTIC")  # 출력용

# CSV 헤더명
COL_DATE = "Date"
COL_NAME = "Stock"
COL_CODE = "Code"
COL_OPEN = "Open"
COL_HIGH = "High"
COL_LOW = "Low"
COL_CLOSE = "Close"
COL_VOLUME = "Volume"

# N건마다 commit (대용량일 때 속도 개선)
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "5000"))


def normalize_ts(ts: str) -> str:
    """
    MySQL DATETIME에 넣기 위한 정규화.
    예: '2025-12-05 10:00:00+00:00' -> '2025-12-05 10:00:00'
    """
    s = str(ts).strip()
    s = s.replace("T", " ")
    s = s.replace("+00:00", "")
    # 만약 끝에 'Z'가 붙는 형식이면 제거
    if s.endswith("Z"):
        s = s[:-1]
    return s


def zfill6(code: str) -> str:
    return str(code).strip().zfill(6)


def to_int_volume(v) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


def to_decimal_2(v) -> Decimal:
    """
    DECIMAL(15,2)에 맞춰 안전 변환.
    빈 값/이상치면 0.00
    """
    try:
        if v is None:
            return Decimal("0.00")
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return Decimal("0.00")
        return Decimal(s).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    stock_id_cache: dict[str, int] = {}

    upsert_stock_sql = """
    INSERT INTO stocks (ticker, name_ko, name_en)
    VALUES (%s, %s, NULL)
    ON DUPLICATE KEY UPDATE
      name_ko = VALUES(name_ko)
    """

    get_stock_id_sql = """
    SELECT id FROM stocks WHERE ticker = %s
    """

    upsert_candle_sql = """
    INSERT INTO stock_price_candles
      (stock_id, timeframe, candle_time,
       open_price, high_price, low_price, close_price, volume)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      open_price  = VALUES(open_price),
      high_price  = VALUES(high_price),
      low_price   = VALUES(low_price),
      close_price = VALUES(close_price),
      volume      = VALUES(volume)
    """

    inserted = 0
    skipped = 0

    try:
        with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    ticker = zfill6(row[COL_CODE])
                    name_ko = (row.get(COL_NAME) or "").strip()
                    candle_time = normalize_ts(row[COL_DATE])

                    o = to_decimal_2(row.get(COL_OPEN))
                    h = to_decimal_2(row.get(COL_HIGH))
                    l = to_decimal_2(row.get(COL_LOW))
                    c = to_decimal_2(row.get(COL_CLOSE))
                    vol = to_int_volume(row.get(COL_VOLUME))

                    # 1) stocks upsert + id 조회(캐시)
                    stock_id = stock_id_cache.get(ticker)
                    if stock_id is None:
                        cur.execute(upsert_stock_sql, (ticker, name_ko))
                        cur.execute(get_stock_id_sql, (ticker,))
                        fetched = cur.fetchone()
                        if not fetched:
                            skipped += 1
                            continue
                        stock_id = int(fetched[0])
                        stock_id_cache[ticker] = stock_id

                    # 2) candles upsert
                    cur.execute(
                        upsert_candle_sql,
                        (stock_id, TIMEFRAME, candle_time, o, h, l, c, vol)
                    )

                    inserted += 1
                    if COMMIT_EVERY > 0 and inserted % COMMIT_EVERY == 0:
                        conn.commit()

                except Exception:
                    skipped += 1
                    continue

        conn.commit()

    finally:
        cur.close()
        conn.close()

    print(f"Imported price data ({MARKET_TYPE}, timeframe={TIMEFRAME})")
    print(f"rows processed={inserted}, skipped={skipped}")


if __name__ == "__main__":
    main()
