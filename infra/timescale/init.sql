CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw metrics from bot fleet (app-layer RTT)
CREATE TABLE IF NOT EXISTS raw_metrics (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    session_id      TEXT        NOT NULL,
    sandbox_id      TEXT        NOT NULL,
    order_id        TEXT        NOT NULL,
    archetype       TEXT        NOT NULL,
    app_rtt_ns      BIGINT,
    kernel_rtt_ns   BIGINT,
    fill_correct    BOOLEAN,
    fill_price      DOUBLE PRECISION,
    fill_qty        BIGINT,
    replay_seq      BIGINT
);

SELECT create_hypertable('raw_metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_raw_metrics_session ON raw_metrics (session_id, time DESC);

-- Composite scores per session (latest score per session)
CREATE TABLE IF NOT EXISTS composite_scores (
    time                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    session_id              TEXT            NOT NULL,
    team_name               TEXT,
    p50_ns                  DOUBLE PRECISION,
    p90_ns                  DOUBLE PRECISION,
    p99_ns                  DOUBLE PRECISION,
    p999_ns                 DOUBLE PRECISION,
    tps                     DOUBLE PRECISION,
    peak_tps                DOUBLE PRECISION,
    fill_accuracy           DOUBLE PRECISION,
    price_time_violations   BIGINT,
    recovery_time_ms        DOUBLE PRECISION,
    degradation_ratio       DOUBLE PRECISION,
    throughput_score        DOUBLE PRECISION,
    tail_latency_score      DOUBLE PRECISION,
    correctness_score       DOUBLE PRECISION,
    resilience_score        DOUBLE PRECISION,
    total_score             DOUBLE PRECISION
);

SELECT create_hypertable('composite_scores', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_scores_session ON composite_scores (session_id, time DESC);

-- Kernel-level RTT events from eBPF prober
CREATE TABLE IF NOT EXISTS kernel_latency (
    time        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    session_id  TEXT            NOT NULL,
    sandbox_id  TEXT            NOT NULL,
    src_ip      TEXT,
    dst_ip      TEXT,
    src_port    INTEGER,
    dst_port    INTEGER,
    rtt_ns      BIGINT          NOT NULL,
    ingress_ns  BIGINT,
    egress_ns   BIGINT
);

SELECT create_hypertable('kernel_latency', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_kernel_session ON kernel_latency (session_id, time DESC);
