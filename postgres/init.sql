-- ─────────────────────────────────────────────
-- CryptoStream - Schema PostgreSQL
-- ─────────────────────────────────────────────

-- Tabela principal de preços brutos
CREATE TABLE IF NOT EXISTS crypto_prices (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    price       NUMERIC(20, 8) NOT NULL,
    volume_24h  NUMERIC(24, 4),
    bid         NUMERIC(20, 8),
    ask         NUMERIC(20, 8),
    event_time  TIMESTAMPTZ    NOT NULL,
    ingested_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Índice para queries de série temporal por símbolo
CREATE INDEX idx_crypto_prices_symbol_time
    ON crypto_prices (symbol, event_time DESC);

-- Tabela de métricas agregadas (saída do Spark)
CREATE TABLE IF NOT EXISTS crypto_metrics (
    id                 BIGSERIAL PRIMARY KEY,
    symbol             VARCHAR(20)    NOT NULL,
    window_start       TIMESTAMPTZ    NOT NULL,
    window_end         TIMESTAMPTZ    NOT NULL,
    avg_price          NUMERIC(20, 8) NOT NULL,
    min_price          NUMERIC(20, 8) NOT NULL,
    max_price          NUMERIC(20, 8) NOT NULL,
    open_price         NUMERIC(20, 8) NOT NULL,
    close_price        NUMERIC(20, 8) NOT NULL,
    trade_count        INTEGER        NOT NULL DEFAULT 0,
    price_change_pct   NUMERIC(10, 4),
    moving_avg_10      NUMERIC(20, 8),
    moving_avg_30      NUMERIC(20, 8),
    std_dev            NUMERIC(20, 8),
    is_anomaly         BOOLEAN        NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_crypto_metrics_symbol_window
    ON crypto_metrics (symbol, window_start DESC);

-- Tabela de alertas disparados
CREATE TABLE IF NOT EXISTS crypto_alerts (
    id           BIGSERIAL PRIMARY KEY,
    symbol       VARCHAR(20)    NOT NULL,
    alert_type   VARCHAR(50)    NOT NULL,  -- PRICE_SPIKE, PRICE_DROP, ANOMALY, VOLUME_SURGE
    message      TEXT           NOT NULL,
    price        NUMERIC(20, 8) NOT NULL,
    change_pct   NUMERIC(10, 4),
    threshold    NUMERIC(10, 4),
    triggered_at TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    acknowledged BOOLEAN        NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_crypto_alerts_symbol_time
    ON crypto_alerts (symbol, triggered_at DESC);

-- View de candlestick para o dashboard
CREATE OR REPLACE VIEW candlestick_1min AS
SELECT
    symbol,
    date_trunc('minute', event_time) AS candle_time,
    (array_agg(price ORDER BY event_time ASC))[1]  AS open,
    MAX(price)                                      AS high,
    MIN(price)                                      AS low,
    (array_agg(price ORDER BY event_time DESC))[1] AS close,
    COUNT(*)                                        AS tick_count
FROM crypto_prices
WHERE event_time > NOW() - INTERVAL '2 hours'
GROUP BY symbol, date_trunc('minute', event_time)
ORDER BY symbol, candle_time DESC;

-- View de médias móveis recentes
CREATE OR REPLACE VIEW moving_averages AS
SELECT
    symbol,
    window_end AS ts,
    avg_price,
    moving_avg_10  AS ma10,
    moving_avg_30  AS ma30,
    price_change_pct,
    is_anomaly
FROM crypto_metrics
WHERE window_end > NOW() - INTERVAL '1 hour'
ORDER BY symbol, window_end DESC;
