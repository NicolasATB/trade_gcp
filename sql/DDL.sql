-- =====================================================================
-- BigQuery DDL - Trading signals pipeline (medallion architecture)
-- Project: trade-390514 | Region: us-central1
--
-- Run with:
--   bq query --use_legacy_sql=false --project_id=trade-390514 < ddl_prod_trade.sql
-- (or paste into the BigQuery console).
--
-- Notes:
--   * Statements are idempotent (IF NOT EXISTS).
--   * All object descriptions are in English.
--   * PRIMARY KEY / FOREIGN KEY are NOT ENFORCED (documentation and optimizer
--     hints only; BigQuery does not enforce them). Remove if undesired.
--   * Create order respects FK references: control + strategy first.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Schemas (datasets)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_control`
OPTIONS (location = 'us-central1',
  description = 'Control layer: operational metadata such as the source registry and load priorities.');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_strategy`
OPTIONS (location = 'us-central1',
  description = 'Strategy layer: strategy catalog and one versioned-parameters table per strategy.');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_bronze`
OPTIONS (location = 'us-central1',
  description = 'Bronze layer: raw landing zone, one table per source/symbol/temporality. Data kept as delivered by the source.');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_silver`
OPTIONS (location = 'us-central1',
  description = 'Silver layer: conformed OHLCV and derived indicator features, generic (symbol and temporality as columns).');

CREATE SCHEMA IF NOT EXISTS `trade-390514.prod_trade_gold`
OPTIONS (location = 'us-central1',
  description = 'Gold layer: business outcomes (trading signals) modeled as a star-schema fact table.');


-- ---------------------------------------------------------------------
-- prod_trade_control
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_control.source_priority` (
  source_id       INT64     NOT NULL OPTIONS(description = "Unique source identifier (logical key)."),
  label           STRING    OPTIONS(description = "Human-readable name of the source."),
  priority        INT64     OPTIONS(description = "Preference order when consolidating multiple sources (higher value is preferred)."),
  is_active       BOOL      OPTIONS(description = "Whether the source is currently active."),
  url_source      STRING    OPTIONS(description = "Download URL/endpoint of the source."),
  name_source     STRING    OPTIONS(description = "Name of the data provider (e.g. CoinAPI, Investing.com)."),
  datetime_update TIMESTAMP OPTIONS(description = "Execution timestamp of the process that wrote the row (audit)."),
  PRIMARY KEY (source_id) NOT ENFORCED
)
OPTIONS(description = "Registry of data sources and their consolidation priority.");


-- ---------------------------------------------------------------------
-- prod_trade_strategy
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_strategy.strategy` (
  strategy_id    INT64     NOT NULL OPTIONS(description = "Unique strategy identifier."),
  strategy_name  STRING    OPTIONS(description = "Human-readable name; equals the name of this strategy's parameter table (e.g. strategy_rsi_daily_week)."),
  description    STRING    OPTIONS(description = "Short description of the strategy."),
  indicator_type STRING    OPTIONS(description = "Base indicator (e.g. RSI)."),
  is_active      BOOL      OPTIONS(description = "Whether the strategy is currently active."),
  created_at     TIMESTAMP OPTIONS(description = "Creation timestamp."),
  PRIMARY KEY (strategy_id) NOT ENFORCED
)
OPTIONS(description = "Catalog of trading strategies.");


-- ---------------------------------------------------------------------
-- prod_trade_bronze (Option A: one table per source/symbol/temporality)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.coinapi_btcusd_daily_raw` (
  source_id         INT64     OPTIONS(description = "FK to prod_trade_control.source."),
  datetime_update   TIMESTAMP OPTIONS(description = "Download execution timestamp (audit)."),
  time_period_start STRING    OPTIONS(description = "Period start as delivered by CoinAPI (raw ISO string)."),
  time_period_end   STRING    OPTIONS(description = "Period end (raw)."),
  time_open         STRING    OPTIONS(description = "Timestamp of first trade in the period (raw)."),
  time_close        STRING    OPTIONS(description = "Timestamp of last trade in the period (raw)."),
  price_open        FLOAT64   OPTIONS(description = "Open price."),
  price_high        FLOAT64   OPTIONS(description = "High price."),
  price_low         FLOAT64   OPTIONS(description = "Low price."),
  price_close       FLOAT64   OPTIONS(description = "Close price."),
  volume_traded     FLOAT64   OPTIONS(description = "Traded volume."),
  trades_count      INT64     OPTIONS(description = "Number of trades in the period."),
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE(datetime_update)
OPTIONS(description = "Raw daily BTC/USD candles from CoinAPI (native format; time fields kept as STRING).");

-- Column names sanitized to BigQuery-safe identifiers; original Spanish names noted in descriptions.
-- id_source and datetime_update added for lineage/audit (not present in the original export).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.investing_btcusd_daily_raw` (
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source (added for lineage)."),
  datetime_update TIMESTAMP OPTIONS(description = "Download execution timestamp (audit; added)."),
  fecha           INT64     OPTIONS(description = "Raw date from Investing.com export, format ddmmyyyy (dmmyyyy when day < 10). Source column: Fecha."),
  ultimo          FLOAT64   OPTIONS(description = "Close price. Source column: Ultimo."),
  apertura        FLOAT64   OPTIONS(description = "Open price. Source column: Apertura."),
  maximo          FLOAT64   OPTIONS(description = "High price. Source column: Maximo."),
  minimo          FLOAT64   OPTIONS(description = "Low price. Source column: Minimo."),
  vol             STRING    OPTIONS(description = "Volume as text with K/M suffixes; needs parsing. Source column: Vol."),
  var_pct         FLOAT64   OPTIONS(description = "Daily percentage change. Source column: % var."),
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE(datetime_update)
OPTIONS(description = "Raw daily BTC/USD candles exported from Investing.com. Column names sanitized to BigQuery-safe identifiers.");


-- Binance candles ingested via CCXT (T-04). CCXT's fetch_ohlcv returns a
-- normalized tuple [open_time_ms, open, high, low, close, volume]; this table
-- stores exactly that, plus lineage/audit columns. Unlike the CoinAPI/Investing
-- bronze tables (partitioned by datetime_update, append-style dumps), this one is
-- partitioned by the candle's own date because the daily ingest upserts (MERGE)
-- on the business key (symbol, candle_date) to stay idempotent.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.binance_btcusd_daily_raw` (
  symbol          STRING    NOT NULL OPTIONS(description = "CCXT unified symbol as requested (e.g. BTC/USDT)."),
  candle_date     DATE      NOT NULL OPTIONS(description = "Candle open date (UTC), derived from open_time. Business key and partition column."),
  open_time       INT64     NOT NULL OPTIONS(description = "Candle open time in epoch milliseconds, as delivered by CCXT/Binance (raw)."),
  price_open      FLOAT64   OPTIONS(description = "Open price."),
  price_high      FLOAT64   OPTIONS(description = "High price."),
  price_low       FLOAT64   OPTIONS(description = "Low price."),
  price_close     FLOAT64   OPTIONS(description = "Close price."),
  volume_traded   FLOAT64   OPTIONS(description = "Base-asset traded volume."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (symbol, candle_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY candle_date
CLUSTER BY symbol
OPTIONS(description = "Raw daily BTC candles from Binance via CCXT. Idempotent daily upsert on (symbol, candle_date).");

-- Register Binance as a source so the bronze FK resolves. Idempotent seed.
-- Priority is illustrative (higher value preferred); adjust to your consolidation policy.
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 3 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (3, 'Binance (CCXT)', 3, TRUE, 'https://api.binance.com', 'Binance', CURRENT_TIMESTAMP());

-- Bitstamp candles ingested via CCXT (same ingest module as Binance, switched
-- by env vars). Extends BTC history before Binance's BTC/USDT listing
-- (2017-08-17): Bitstamp trades BTC/USD continuously since ~2011-08. Same
-- shape and upsert pattern as the Binance table: partitioned by the candle's
-- own date, idempotent MERGE on (symbol, candle_date). Cut-over policy:
-- Bitstamp rows are loaded only up to 2017-08-16; Binance covers 2017-08-17
-- onward, so the two sources never overlap by date.
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_bronze.bitstamp_btcusd_daily_raw` (
  symbol          STRING    NOT NULL OPTIONS(description = "CCXT unified symbol as requested (e.g. BTC/USD)."),
  candle_date     DATE      NOT NULL OPTIONS(description = "Candle open date (UTC), derived from open_time. Business key and partition column."),
  open_time       INT64     NOT NULL OPTIONS(description = "Candle open time in epoch milliseconds, as delivered by CCXT/Bitstamp (raw)."),
  price_open      FLOAT64   OPTIONS(description = "Open price."),
  price_high      FLOAT64   OPTIONS(description = "High price."),
  price_low       FLOAT64   OPTIONS(description = "Low price."),
  price_close     FLOAT64   OPTIONS(description = "Close price."),
  volume_traded   FLOAT64   OPTIONS(description = "Base-asset traded volume."),
  source_id       INT64     OPTIONS(description = "FK to prod_trade_control.source_priority."),
  datetime_update TIMESTAMP OPTIONS(description = "Download/upsert execution timestamp (audit)."),
  PRIMARY KEY (symbol, candle_date) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY candle_date
CLUSTER BY symbol
OPTIONS(description = "Raw daily BTC candles from Bitstamp via CCXT (pre-Binance history, up to 2017-08-16). Idempotent upsert on (symbol, candle_date).");

-- Register Bitstamp as a source. Idempotent seed. Priority 4: preferred over
-- Binance (3) on consolidation ties (moot in practice — the date cut-over
-- keeps the two sources disjoint).
MERGE `trade-390514.prod_trade_control.source_priority` T
USING (SELECT 4 AS source_id) S
ON T.source_id = S.source_id
WHEN NOT MATCHED THEN INSERT (source_id, label, priority, is_active, url_source, name_source, datetime_update)
VALUES (4, 'Bitstamp (CCXT)', 4, TRUE, 'https://www.bitstamp.net/api/', 'Bitstamp', CURRENT_TIMESTAMP());


-- ---------------------------------------------------------------------
-- prod_trade_silver (Option B: symbol and temporality as columns)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_silver.ohlcv_validated` (
  symbol            STRING    NOT NULL OPTIONS(description = "Asset (e.g. BTCUSD)."),
  temporality       STRING    NOT NULL OPTIONS(description = "Timeframe: 1d / 1h / 1w."),
  source_id         INT64     OPTIONS(description = "Lineage: source of the winning row. FK to prod_trade_control.source."),
  datetime_update   TIMESTAMP OPTIONS(description = "Execution timestamp of the conforming process (audit)."),
  time_period_start TIMESTAMP NOT NULL OPTIONS(description = "Period start (UTC). Temporal key and partition column."),
  time_period_end   TIMESTAMP OPTIONS(description = "Period end (UTC)."),
  time_open         TIMESTAMP OPTIONS(description = "Timestamp of first trade (UTC)."),
  time_close        TIMESTAMP OPTIONS(description = "Timestamp of last trade (UTC)."),
  price_open        FLOAT64   OPTIONS(description = "Open price."),
  price_high        FLOAT64   OPTIONS(description = "High price."),
  price_low         FLOAT64   OPTIONS(description = "Low price."),
  price_close       FLOAT64   OPTIONS(description = "Close price."),
  volume_traded     FLOAT64   OPTIONS(description = "Traded volume."),
  trades_count      INT64     OPTIONS(description = "Number of trades."),
  PRIMARY KEY (symbol, temporality, time_period_start) NOT ENFORCED,
  FOREIGN KEY (source_id) REFERENCES `trade-390514.prod_trade_control.source_priority`(source_id) NOT ENFORCED
)
PARTITION BY DATE(time_period_start)
CLUSTER BY symbol, temporality
OPTIONS(description = "Conformed, typed and de-duplicated OHLCV (one row per symbol/temporality/period), chosen by source priority.");

CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_silver.rsi_features` (
  symbol            STRING    NOT NULL OPTIONS(description = "Asset."),
  temporality       STRING    NOT NULL OPTIONS(description = "Timeframe: 1d / 1h / 1w."),
  rsi_period        INT64     NOT NULL OPTIONS(description = "RSI period used to compute the row."),
  datetime_update   TIMESTAMP OPTIONS(description = "Execution timestamp of the computation (audit)."),
  time_period_start TIMESTAMP NOT NULL OPTIONS(description = "Temporal key of the value (UTC)."),
  price_close       FLOAT64   OPTIONS(description = "Close price used in the computation."),
  var_p_recursive   FLOAT64   OPTIONS(description = "Wilder smoothed average gain (recursive state)."),
  var_n_recursive   FLOAT64   OPTIONS(description = "Wilder smoothed average loss (recursive state)."),
  rsi               FLOAT64   OPTIONS(description = "RSI value."),
  PRIMARY KEY (symbol, temporality, rsi_period, time_period_start) NOT ENFORCED
)
PARTITION BY DATE(time_period_start)
CLUSTER BY symbol, temporality
OPTIONS(description = "RSI feature computed with Wilder smoothing; recursive intermediate state stored for incremental, idempotent updates. Reusable across strategies.");


-- ---------------------------------------------------------------------
-- prod_trade_gold (star schema)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_gold.fact_signals` (
  symbol            STRING    NOT NULL OPTIONS(description = "Asset."),
  temporality       STRING    NOT NULL OPTIONS(description = "Timeframe: 1d / 1h / 1w."),
  strategy_id       INT64     NOT NULL OPTIONS(description = "FK to prod_trade_strategy.strategy."),
  signal            STRING    OPTIONS(description = "Discrete signal: BUY / SELL / NEUTRAL (enforced by convention)."),
  trigger_params    JSON      OPTIONS(description = "Strategy-agnostic parameter:value dictionary that triggered the signal change (e.g. {\"rsi\":29.8,\"oversold\":30}). Allows non-RSI strategies to record their own trigger inputs."),
  signal_start TIMESTAMP NOT NULL OPTIONS(description = "Period the signal corresponds to (UTC)."),
  signal_created_at       TIMESTAMP OPTIONS(description = "When the signal was computed."),
  PRIMARY KEY (symbol, temporality, signal_start, strategy_id) NOT ENFORCED,
  FOREIGN KEY (strategy_id) REFERENCES `trade-390514.prod_trade_strategy.strategy`(strategy_id) NOT ENFORCED
)
PARTITION BY DATE(signal_start)
CLUSTER BY symbol, strategy_id
OPTIONS(description = "Trading signals fact table: one row per symbol/temporality/period/strategy.");


-- ---------------------------------------------------------------------
-- prod_trade_strategy — versioned RSI parameters (T-06 / T-07)
-- One table per strategy; name matches strategy.strategy_name.
-- ---------------------------------------------------------------------
-- Strategy: combines weekly RSI (trend filter) + daily RSI (entry/exit signal).
-- weekly_rsi_trend_start / _end define the bullish window (same convention as
-- the AG notebook: trend is active while weekly RSI sits between these bounds).
-- daily_rsi_oversold / _overbought are the entry/exit thresholds for daily RSI.
-- The four parameters are calibrated offline by the genetic algorithm (AG).
CREATE TABLE IF NOT EXISTS `trade-390514.prod_trade_strategy.strategy_rsi_daily_week` (
  param_version          INT64     NOT NULL OPTIONS(description = "Monotonically increasing version; latest active version is used."),
  is_active              BOOL      NOT NULL OPTIONS(description = "Only the active version is applied by the signal pipeline."),
  rsi_period             INT64     NOT NULL OPTIONS(description = "RSI lookback period (Wilder smoothing), applied to both daily and weekly."),
  weekly_rsi_trend_start FLOAT64   NOT NULL OPTIONS(description = "Weekly RSI lower bound of the bullish window (0-100). Trend is considered active when weekly RSI >= this value."),
  weekly_rsi_trend_end   FLOAT64   NOT NULL OPTIONS(description = "Weekly RSI upper bound. Trend ends when weekly RSI exceeds this value."),
  daily_rsi_oversold     FLOAT64   NOT NULL OPTIONS(description = "Daily RSI strictly below this value triggers BUY (within the trend window)."),
  daily_rsi_overbought   FLOAT64   NOT NULL OPTIONS(description = "Daily RSI strictly above this value triggers SELL (within or at end of trend window)."),
  created_at             TIMESTAMP OPTIONS(description = "Row creation timestamp."),
  notes                  STRING    OPTIONS(description = "Rationale or source for these parameters."),
  PRIMARY KEY (param_version) NOT ENFORCED
)
OPTIONS(description = "Versioned parameters for the weekly-trend + daily-RSI strategy. Calibrated by the AG notebook; decoupled from the daily signal pipeline.");

-- Register strategy id=1 in the catalog (idempotent).
MERGE `trade-390514.prod_trade_strategy.strategy` T
USING (SELECT 1 AS strategy_id) S
ON T.strategy_id = S.strategy_id
WHEN NOT MATCHED THEN INSERT (
  strategy_id, strategy_name, description, indicator_type, is_active, created_at
) VALUES (
  1,
  'strategy_rsi_daily_week',
  'Bullish-trend filter (weekly RSI) + entry/exit thresholds (daily RSI). Parameters calibrated by a genetic algorithm.',
  'RSI',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Seed default parameters v1 (idempotent).
-- Baseline values (replace with AG-calibrated output once the notebook is run).
MERGE `trade-390514.prod_trade_strategy.strategy_rsi_daily_week` T
USING (SELECT 1 AS param_version) S
ON T.param_version = S.param_version
WHEN NOT MATCHED THEN INSERT (
  param_version, is_active, rsi_period,
  weekly_rsi_trend_start, weekly_rsi_trend_end,
  daily_rsi_oversold, daily_rsi_overbought,
  created_at, notes
) VALUES (
  1, TRUE, 14,
  40.0, 70.0,
  30.0, 70.0,
  CURRENT_TIMESTAMP(),
  'Baseline seed — replace with AG-optimised values from ag-determina-parametros-de-estrategia-rsi.ipynb.'
);