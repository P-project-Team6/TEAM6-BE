import csv
import mysql.connector

DB = {
    "host": "localhost",
    "user": "root",
    "password": "1047",
    "database": "finsight",
}

CSV_PATH = "stock_price_data_top80.csv"

TIMEFRAME = "1H"          # 1H / 1D 등
MARKET_TYPE = "DOMESTIC"  # 지금 데이터는 전부 국내 주식

# CSV 헤더명
COL_DATE = "Date"
COL_NAME = "Stock"
COL_CODE = "Code"
COL_OPEN = "Open"
COL_HIGH = "High"
COL_LOW = "Low"
COL_CLOSE = "Close"
COL_VOLUME = "Volume"

def normalize_ts(ts: str) -> str:
    return str(ts).replace("+00:00", "").strip()

def zfill6(code: str) -> str:
    return str(code).strip().zfill(6)

def to_int_volume(v) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0

def main():
    conn = mysql.connector.connect(**DB)
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
      open_price = VALUES(open_price),
      high_price = VALUES(high_price),
      low_price  = VALUES(low_price),
      close_price= VALUES(close_price),
      volume     = VALUES(volume)
    """

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            ticker = zfill6(row[COL_CODE])
            name_ko = row[COL_NAME]
            candle_time = normalize_ts(row[COL_DATE])

            o = row[COL_OPEN]
            h = row[COL_HIGH]
            l = row[COL_LOW]
            c = row[COL_CLOSE]
            vol = to_int_volume(row[COL_VOLUME])

            # 1️. stocks 처리
            stock_id = stock_id_cache.get(ticker)
            if stock_id is None:
                cur.execute(upsert_stock_sql, (ticker, name_ko))
                cur.execute(get_stock_id_sql, (ticker,))
                stock_id = cur.fetchone()[0]
                stock_id_cache[ticker] = stock_id

            # 2️. 가격 데이터 처리
            cur.execute(
                upsert_candle_sql,
                (stock_id, TIMEFRAME, candle_time, o, h, l, c, vol)
            )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Imported price data ({MARKET_TYPE}, timeframe={TIMEFRAME})")

if __name__ == "__main__":
    main()
