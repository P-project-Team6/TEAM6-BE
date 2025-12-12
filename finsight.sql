CREATE DATABASE IF NOT EXISTS finsight
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE finsight;

CREATE TABLE IF NOT EXISTS sources (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code       VARCHAR(20) NOT NULL UNIQUE COMMENT 'NAVER, KAKAO, 증권PLUS 등',
    name       VARCHAR(100) NOT NULL COMMENT '출처 이름',
    base_url   VARCHAR(255) NULL COMMENT '기본 URL'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO sources (id,code, name, base_url)
VALUES
  (1,'NAVER', '네이버 증권', 'https://finance.naver.com'),
  (2,'KAKAO', '카카오페이 증권', NULL),
  (3,'SPLUS', '증권PLUS', NULL);

CREATE TABLE IF NOT EXISTS users (
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email          VARCHAR(255) NOT NULL UNIQUE COMMENT '로그인용 이메일',
    password_hash  VARCHAR(255) NOT NULL COMMENT '비밀번호 해시',
    nickname       VARCHAR(50) NOT NULL COMMENT '표시 이름',
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stocks (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    ticker          VARCHAR(16) NOT NULL UNIQUE COMMENT '종목 코드',
    name_ko         VARCHAR(100) NOT NULL COMMENT '한글 종목명',
    name_en         VARCHAR(100) NULL COMMENT '영문 명칭'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
--  나중에 필요시 (market VARCHAR(20) NOT NULL COMMENT 'KOSPI, KOSDAQ, ETF 등'),(sector VARCHAR(100) NULL COMMENT '업종/섹터') 추가

CREATE TABLE IF NOT EXISTS stock_price_candles (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    stock_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK → stocks.id',

    -- 캔들 간격 (1H, 1D, 3D, 1W 등)
    timeframe    VARCHAR(10) NOT NULL COMMENT '캔들 간격',

    -- 캔들 기준 시각
    -- 1H: 해당 시간의 시작 시각
    -- 1D: 해당 일자의 00:00
    candle_time  DATETIME NOT NULL COMMENT '캔들 기준 시각',

    open_price   DECIMAL(15, 2) NOT NULL COMMENT '시가',
    high_price   DECIMAL(15, 2) NOT NULL COMMENT '고가',
    low_price    DECIMAL(15, 2) NOT NULL COMMENT '저가',
    close_price  DECIMAL(15, 2) NOT NULL COMMENT '종가',
    volume       BIGINT UNSIGNED NOT NULL COMMENT '거래량',

    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_candle_stock
        FOREIGN KEY (stock_id) REFERENCES stocks(id)
        ON DELETE CASCADE,
        
    CONSTRAINT uq_candle_unique
        UNIQUE (stock_id, timeframe, candle_time),
        
    INDEX idx_candle_stock_time (stock_id, timeframe, candle_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_favorites (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id    BIGINT UNSIGNED NOT NULL COMMENT 'FK → users.id',
    stock_id   BIGINT UNSIGNED NOT NULL COMMENT 'FK → stocks.id',

    CONSTRAINT fk_fav_user
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_fav_stock
        FOREIGN KEY (stock_id) REFERENCES stocks(id)
        ON DELETE CASCADE,

    CONSTRAINT uq_user_stock
        UNIQUE (user_id, stock_id),

    INDEX idx_fav_user (user_id),
    INDEX idx_fav_stock (stock_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS external_posts (
    id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source_id         BIGINT UNSIGNED NOT NULL COMMENT 'FK → sources.id',
    -- 원본 사이트의 게시글 식별자
    external_post_id  VARCHAR(100) NOT NULL COMMENT '원본 사이트 글 ID',

    stock_id          BIGINT UNSIGNED NOT NULL COMMENT 'FK → stocks.id',
    market_type       ENUM('DOMESTIC', 'OVERSEAS') NOT NULL COMMENT '국내/해외',
    
    title             VARCHAR(255) NOT NULL COMMENT '게시글 제목',
    like_count        INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '찬성/공감',
    dislike_count     INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '반대/비공감',
    view_count        INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '조회수',
    url               VARCHAR(500) NOT NULL COMMENT '원문 URL',
    posted_at         DATETIME NOT NULL COMMENT '원문 게시 시각',

    -- FK 제약
    CONSTRAINT fk_ext_posts_source
        FOREIGN KEY (source_id) REFERENCES sources(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_ext_posts_stock
        FOREIGN KEY (stock_id) REFERENCES stocks(id)
        ON DELETE CASCADE,

    CONSTRAINT uq_source_external_post
        UNIQUE (source_id, external_post_id),

    INDEX idx_posts_stock_time (stock_id, posted_at),
    INDEX idx_posts_source_time (source_id, posted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_daily_recommendations (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

    stock_id BIGINT UNSIGNED NOT NULL COMMENT 'FK → stocks.id',
    source_id BIGINT UNSIGNED NOT NULL COMMENT 'FK → sources.id',

    signal_date DATE NOT NULL COMMENT '추천 기준 날짜(분석한 날짜)',

    positive_ratio FLOAT NOT NULL COMMENT '긍정 비율 (0~1)',
    threshold_used FLOAT NOT NULL COMMENT '추천 기준값',

    is_recommended TINYINT NOT NULL COMMENT 'positive_ratio > threshold_used',
    actual_is_up   TINYINT NULL COMMENT '다음 거래일 상승 여부',
    is_hit         TINYINT NULL COMMENT '추천 적중 여부(추천한 경우만)',

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_rec_stock
        FOREIGN KEY (stock_id) REFERENCES stocks(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_rec_source
        FOREIGN KEY (source_id) REFERENCES sources(id)
        ON DELETE CASCADE,

    CONSTRAINT uq_daily_stock_source
        UNIQUE (stock_id, source_id, signal_date),

    INDEX idx_rec_stock_date (stock_id, signal_date),
    INDEX idx_rec_source_date (source_id, signal_date),
    INDEX idx_rec_date (signal_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;