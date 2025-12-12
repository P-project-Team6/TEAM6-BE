import csv
import mysql.connector

DB = {
    "host": "localhost",
    "user": "root",
    "password": "1047",
    "database": "finsight",
}

CSV_PATH = "prediction_result_report.csv"

# 최종 선택할 best_threshold 값으로 나중에 변경
THRESHOLD_USED = 0.35

# CSV 헤더
COL_DATE = "Date"
COL_CODE = "Code"
COL_TYPE = "Type"
COL_POS_RATIO = "Positive_Ratio"
COL_PRED_SUCCESS = "Prediction_Success"

def zfill6(code: str) -> str:
    return str(code).strip().zfill(6)

def main():
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()
    
    # NAVER source_id 고정 (임시) -> 나중에 추가할 예정
    cur.execute("SELECT id FROM sources WHERE code = 'NAVER'")
    source_id = cur.fetchone()[0]
    
    # 캐시
    stock_id_cache = {}
    source_id_cache = {}

    # sources 미리 로딩 -> 나중에 교체 예정
    cur.execute("SELECT id, code FROM sources")
    for sid, code in cur.fetchall():
        source_id_cache[str(code).strip()] = int(sid)

    get_stock_id_sql = "SELECT id FROM stocks WHERE ticker = %s"

    upsert_sql = """
    INSERT INTO stock_daily_recommendations
      (stock_id, source_id, signal_date,
       positive_ratio, threshold_used,
       is_recommended, actual_is_up, is_hit)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      positive_ratio = VALUES(positive_ratio),
      threshold_used = VALUES(threshold_used),
      is_recommended = VALUES(is_recommended),
      actual_is_up   = VALUES(actual_is_up),
      is_hit         = VALUES(is_hit)
    """

    inserted = 0
    skipped_stock = 0
    skipped_source = 0

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            signal_date = str(row[COL_DATE]).strip()  # 'YYYY-MM-DD' 형태라고 가정
            ticker = zfill6(row[COL_CODE])
            pos_ratio = float(row[COL_POS_RATIO])

            # Prediction_Success는 다음 거래일 상승 여부를 뜻함
            pred = str(row[COL_PRED_SUCCESS]).strip().lower()
            actual_is_up = 1 if pred == "success" else 0

            is_recommended = 1 if (pos_ratio > THRESHOLD_USED) else 0
            is_hit = actual_is_up if is_recommended == 1 else None  # 추천한 것만 평가

            # stock_id 조회
            stock_id = stock_id_cache.get(ticker)
            if stock_id is None:
                cur.execute(get_stock_id_sql, (ticker,))
                r = cur.fetchone()
                if not r:
                    skipped_stock += 1
                    continue
                stock_id = int(r[0])
                stock_id_cache[ticker] = stock_id

            cur.execute(
                upsert_sql,
                (stock_id, source_id, signal_date,
                 pos_ratio, THRESHOLD_USED,
                 is_recommended, actual_is_up, is_hit)
            )
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Done. upserted={inserted}, skipped_stock={skipped_stock}, skipped_source={skipped_source}")

if __name__ == "__main__":
    main()
