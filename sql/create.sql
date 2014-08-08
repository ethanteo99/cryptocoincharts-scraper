CREATE TABLE IF NOT EXISTS exchange_pair_hour (
    exchange VARCHAR(20),
    source VARCHAR(10),
    sink VARCHAR(10),
    hour TIMESTAMP,
    price_low DECIMAL,
    price_25th_percentile DECIMAL,
    price_75th_percentile DECIMAL,
    price_high DECIMAL,
    price_median DECIMAL,
    price_ema20 DECIMAL,
    volume DECIMAL,
    field_7 DECIMAL,
    field_8 DECIMAL);

CREATE INDEX ON exchange_pair_hour (exchange, source, sink, hour);
CREATE INDEX ON exchange_pair_hour (source, sink, hour);
CREATE INDEX ON exchange_pair_hour (sink, hour);
CREATE INDEX ON exchange_pair_hour (exchange, hour);