import os
import csv
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

CSV_PATH = os.getenv("REC_CSV", "prediction_result_report.csv")
THRESHOLD_USED = float(os.getenv("THRESHOLD_USED", "0.35"))

# CSV 헤더
COL_DATE = "Date"
COL_CODE = "Code"
COL_POS_RATIO = "Positive_Ratio"
COL_PRED_SUCCESS = "Prediction_Success"

COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "5000"))


def zfill6(code: str) -> str:
    return str(code).strip().zfill(6)


def parse_date_yyyy_mm_dd(s: str) -> str | None:
    s = str(s).strip()
    if not s:
        return None
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_success_flag(v) -> int | None:
    """
    Prediction_Success:
      - success -> 1 (예측 맞음)
      - fail    -> 0 (예측 틀림)
      - 그 외   -> None
    """
    if v is None:
        return None
    s = str(v).strip().lower()
    if s == "success":
        return 1
    if s == "fail":
        return 0
    return None


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    # 이번 모델: NAVER 고정
    cur.execute("SELECT id FROM sources WHERE code='NAVER' LIMIT 1;")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("sources 테이블에 code='NAVER'가 없습니다.")
    source_id = int(row[0])

    stock_id_cache: dict[str, int] = {}
    get_stock_id_sql = "SELECT id FROM stocks WHERE ticker = %s LIMIT 1;"

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

    upserted = 0
    skipped_stock = 0
    skipped_row = 0

    try:
        with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for r in reader:
                signal_date = parse_date_yyyy_mm_dd(r.get(COL_DATE))
                if not signal_date:
                    skipped_row += 1
                    continue

                ticker = zfill6(r.get(COL_CODE))

                try:
                    pos_ratio = float(r.get(COL_POS_RATIO))
                except Exception:
                    skipped_row += 1
                    continue

                is_recommended = 1 if (pos_ratio > THRESHOLD_USED) else 0


                success_flag = parse_success_flag(r.get(COL_PRED_SUCCESS))

                actual_is_up = None
                if success_flag is not None:
                    if is_recommended == 1:
                        actual_is_up = success_flag
                    else:
                        actual_is_up = 0 if success_flag == 1 else 1

                is_hit = None
                if is_recommended == 1 and success_flag is not None:
                    is_hit = success_flag  # 정답 여부와 동일

                # stock_id 조회
                stock_id = stock_id_cache.get(ticker)
                if stock_id is None:
                    cur.execute(get_stock_id_sql, (ticker,))
                    srow = cur.fetchone()
                    if not srow:
                        skipped_stock += 1
                        continue
                    stock_id = int(srow[0])
                    stock_id_cache[ticker] = stock_id

                cur.execute(
                    upsert_sql,
                    (
                        stock_id, source_id, signal_date,
                        pos_ratio, THRESHOLD_USED,
                        is_recommended, actual_is_up, is_hit
                    )
                )
                upserted += 1

                if COMMIT_EVERY > 0 and upserted % COMMIT_EVERY == 0:
                    conn.commit()

        conn.commit()

    finally:
        cur.close()
        conn.close()

    print(f"Done. upserted={upserted}, skipped_stock={skipped_stock}, skipped_row={skipped_row}")


if __name__ == "__main__":
    main()
