from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

try:
    import psycopg2  # type: ignore
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

_log = logging.getLogger(__name__)

_DDL_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals (
    id                        BIGSERIAL PRIMARY KEY,
    timestamp                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol                    TEXT,
    contract_mode             TEXT,
    entry_epoch               BIGINT,
    direction                 TEXT,
    score                     INTEGER,
    stake                     DOUBLE PRECISION,
    dry_run                   BOOLEAN,
    bb_width_percent          DOUBLE PRECISION,
    tick_atr_percent          DOUBLE PRECISION,
    recent_move_percent       DOUBLE PRECISION,
    hurst_exponent            DOUBLE PRECISION,
    tick_imbalance            DOUBLE PRECISION,
    hawkes_intensity          DOUBLE PRECISION,
    velocity_zscore           DOUBLE PRECISION,
    acceleration_zscore       DOUBLE PRECISION,
    pmi_distance_percent      DOUBLE PRECISION,
    markov_p_up_given_up      DOUBLE PRECISION,
    markov_p_down_given_down  DOUBLE PRECISION,
    shannon_entropy           DOUBLE PRECISION,
    kalman_residual_zscore    DOUBLE PRECISION,
    bayesian_prob_up          DOUBLE PRECISION,
    renyi_entropy             DOUBLE PRECISION,
    fisher_information        DOUBLE PRECISION,
    wavelet_energy_ratio      DOUBLE PRECISION,
    cusum_score               DOUBLE PRECISION,
    tail_dependence           DOUBLE PRECISION,
    mi_flow                   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS signals_entry_epoch_idx ON signals (entry_epoch);
"""

_DDL_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id                        BIGSERIAL PRIMARY KEY,
    timestamp                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol                    TEXT,
    contract_mode             TEXT,
    contract_id               BIGINT,
    entry_epoch               BIGINT,
    exit_epoch                BIGINT,
    held_ticks                INTEGER,
    direction                 TEXT,
    score                     INTEGER,
    soros_step                INTEGER,
    gale_step                 INTEGER,
    stake                     DOUBLE PRECISION,
    buy_price                 DOUBLE PRECISION,
    profit                    DOUBLE PRECISION,
    result                    TEXT,
    bb_width_percent          DOUBLE PRECISION,
    tick_atr_percent          DOUBLE PRECISION,
    recent_move_percent       DOUBLE PRECISION,
    hurst_exponent            DOUBLE PRECISION,
    tick_imbalance            DOUBLE PRECISION,
    hawkes_intensity          DOUBLE PRECISION,
    velocity_zscore           DOUBLE PRECISION,
    acceleration_zscore       DOUBLE PRECISION,
    pmi_distance_percent      DOUBLE PRECISION,
    markov_p_up_given_up      DOUBLE PRECISION,
    markov_p_down_given_down  DOUBLE PRECISION,
    shannon_entropy           DOUBLE PRECISION,
    kalman_residual_zscore    DOUBLE PRECISION,
    bayesian_prob_up          DOUBLE PRECISION,
    renyi_entropy             DOUBLE PRECISION,
    fisher_information        DOUBLE PRECISION,
    wavelet_energy_ratio      DOUBLE PRECISION,
    cusum_score               DOUBLE PRECISION,
    tail_dependence           DOUBLE PRECISION,
    mi_flow                   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS trades_entry_epoch_idx ON trades (entry_epoch);
CREATE INDEX IF NOT EXISTS trades_result_idx ON trades (result);
"""

_SQL_INSERT_SIGNAL = """
INSERT INTO signals (
    timestamp, symbol, contract_mode, entry_epoch, direction, score, stake, dry_run,
    bb_width_percent, tick_atr_percent, recent_move_percent, hurst_exponent,
    tick_imbalance, hawkes_intensity, velocity_zscore, acceleration_zscore,
    pmi_distance_percent, markov_p_up_given_up, markov_p_down_given_down,
    shannon_entropy, kalman_residual_zscore,
    bayesian_prob_up, renyi_entropy, fisher_information,
    wavelet_energy_ratio, cusum_score, tail_dependence, mi_flow
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

_SQL_INSERT_TRADE = """
INSERT INTO trades (
    timestamp, symbol, contract_mode, contract_id, entry_epoch, exit_epoch,
    held_ticks, direction, score, soros_step, gale_step, stake, buy_price,
    profit, result,
    bb_width_percent, tick_atr_percent, recent_move_percent, hurst_exponent,
    tick_imbalance, hawkes_intensity, velocity_zscore, acceleration_zscore,
    pmi_distance_percent, markov_p_up_given_up, markov_p_down_given_down,
    shannon_entropy, kalman_residual_zscore,
    bayesian_prob_up, renyi_entropy, fisher_information,
    wavelet_energy_ratio, cusum_score, tail_dependence, mi_flow
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


class TradeJournal:
    def __init__(self, pg_dsn: str = ""):
        self._pg_dsn = pg_dsn
        self._schema_ready = False
        if pg_dsn and _HAS_PSYCOPG2:
            self._ensure_schema()
        elif pg_dsn and not _HAS_PSYCOPG2:
            _log.warning("PG_DSN definido mas psycopg2 nao instalado. Instale: pip install psycopg2-binary")

    def _connect(self):
        return psycopg2.connect(self._pg_dsn)

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(_DDL_SIGNALS)
                    cur.execute(_DDL_TRADES)
            self._schema_ready = True
        except Exception as exc:
            _log.error("TradeJournal schema error: %s", exc)

    @staticmethod
    def _metric(metrics: dict[str, Any] | None, name: str) -> float | None:
        if not metrics or name not in metrics:
            return None
        try:
            v = float(metrics[name])
        except (TypeError, ValueError):
            return None
        return None if v != v else v  # NaN → None

    def log_signal(
        self,
        symbol: str,
        contract_mode: str,
        entry_epoch: int,
        direction: str,
        score: int,
        stake: float,
        dry_run: bool,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        if not self._pg_dsn or not _HAS_PSYCOPG2:
            return
        self._ensure_schema()
        m = metrics
        row = (
            datetime.now(UTC),
            symbol, contract_mode, entry_epoch, direction, score, stake, dry_run,
            self._metric(m, "bb_width_percent"),
            self._metric(m, "tick_atr_percent"),
            self._metric(m, "recent_move_percent"),
            self._metric(m, "hurst_exponent"),
            self._metric(m, "tick_imbalance"),
            self._metric(m, "hawkes_intensity"),
            self._metric(m, "velocity_zscore"),
            self._metric(m, "acceleration_zscore"),
            self._metric(m, "pmi_distance_percent"),
            self._metric(m, "markov_p_up_given_up"),
            self._metric(m, "markov_p_down_given_down"),
            self._metric(m, "shannon_entropy"),
            self._metric(m, "kalman_residual_zscore"),
            self._metric(m, "bayesian_prob_up"),
            self._metric(m, "renyi_entropy"),
            self._metric(m, "fisher_information"),
            self._metric(m, "wavelet_energy_ratio"),
            self._metric(m, "cusum_score"),
            self._metric(m, "tail_dependence"),
            self._metric(m, "mi_flow"),
        )
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(_SQL_INSERT_SIGNAL, row)
        except Exception as exc:
            _log.error("log_signal PG error: %s", exc)

    def log_trade(
        self,
        symbol: str,
        contract_mode: str,
        contract_id: int,
        entry_epoch: int,
        direction: str,
        score: int,
        stake: float,
        buy_price: float,
        profit: float,
        exit_epoch: int | None = None,
        held_ticks: int | None = None,
        metrics: dict[str, Any] | None = None,
        soros_step: int = 0,
        gale_step: int = 0,
    ) -> None:
        if not self._pg_dsn or not _HAS_PSYCOPG2:
            return
        self._ensure_schema()
        result = "WIN" if profit > 0 else "LOSS"
        m = metrics
        row = (
            datetime.now(UTC),
            symbol, contract_mode, contract_id, entry_epoch, exit_epoch, held_ticks,
            direction, score, soros_step, gale_step, stake, buy_price, profit, result,
            self._metric(m, "bb_width_percent"),
            self._metric(m, "tick_atr_percent"),
            self._metric(m, "recent_move_percent"),
            self._metric(m, "hurst_exponent"),
            self._metric(m, "tick_imbalance"),
            self._metric(m, "hawkes_intensity"),
            self._metric(m, "velocity_zscore"),
            self._metric(m, "acceleration_zscore"),
            self._metric(m, "pmi_distance_percent"),
            self._metric(m, "markov_p_up_given_up"),
            self._metric(m, "markov_p_down_given_down"),
            self._metric(m, "shannon_entropy"),
            self._metric(m, "kalman_residual_zscore"),
            self._metric(m, "bayesian_prob_up"),
            self._metric(m, "renyi_entropy"),
            self._metric(m, "fisher_information"),
            self._metric(m, "wavelet_energy_ratio"),
            self._metric(m, "cusum_score"),
            self._metric(m, "tail_dependence"),
            self._metric(m, "mi_flow"),
        )
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(_SQL_INSERT_TRADE, row)
        except Exception as exc:
            _log.error("log_trade PG error: %s", exc)
