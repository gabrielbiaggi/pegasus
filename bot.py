from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from config import BotConfig, load_config
import deriv_auth
from journal import TradeJournal
from logger import logger
from risk_manager import RiskManager
from strategy import (
    EnsembleScorer,
    EnsembleScorerRF,
    JumpMomentumConfig,
    calculate_tick_indicators,
    generate_accumulator_signal,
    generate_calm_accu_signal,
    generate_jump_momentum_signal,
    generate_rise_fall_signal,
)

#: Maximum acceptable tick age in seconds before an entry is skipped.
#: Configurable via MAX_TICK_LATENCY_MS env var (default 2000ms to handle
#: real-world network latency between Deriv servers and the bot host).
MAX_TICK_LATENCY_SECONDS: float = (
    float(os.getenv("MAX_TICK_LATENCY_MS", "2000")) / 1000.0
)


class FatalBotError(RuntimeError):
    pass


def get_symbol_median_volatility(symbol: str) -> float:
    symbol_upper = symbol.upper()
    baselines = {
        "BOOM1000": 1.0e-6,
        "1HZ100V": 1.4e-4,
        "1HZ10V": 1.5e-5,
    }
    return baselines.get(symbol_upper, 1.4e-4)


@dataclass
class PendingOrder:
    stake: float
    score: int
    entry_epoch: int
    metrics: dict[str, Any] | None = None
    direction: str = "ACCU"  # "ACCU", "CALL", or "PUT"


class DerivBot:
    def __init__(self, config: BotConfig):
        self.config = config
        self.tick_buffer: deque[dict[str, Any]] = deque(maxlen=config.tick_count + 5)
        self.risk: Optional[RiskManager] = None
        self.pending_order: Optional[PendingOrder] = None
        self.waiting_for_result = False
        self.current_contract_id: Optional[int] = None
        self.settled_contract_ids: set[int] = set()
        self.last_accumulator_entry_epoch: Optional[int] = None
        self.accumulator_open_epoch: Optional[int] = None
        self.accumulator_sell_requested = False
        # Saved on disconnect so portfolio-empty handler can account for a trade
        # that settled while the WebSocket was down.
        self._stale_pending_order: Optional[PendingOrder] = None
        self.journal = TradeJournal(config.pg_dsn, journal_dir=config.journal_dir)
        # Load XGBoost ensemble scorer if enabled
        self._ensemble_scorer: EnsembleScorer | None = None
        if config.accumulator_use_ensemble:
            try:
                self._ensemble_scorer = EnsembleScorer()
                logger.info("EnsembleScorer XGBoost carregado para producao.")
            except Exception as exc:
                logger.warning("EnsembleScorer nao carregado: %s", exc)
        self._tick_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=500)
        self._waiting_since: float = 0.0
        self._gale_wait_log_ts: float = 0.0  # throttle log para aguardo do último gale
        self._gale_wait_ticks: int = 0  # contador de ticks em modo wait do último gale
        self._sniper_wait_start: float = (
            0.0  # monotonic time when current gale step was first blocked by SNIPER
        )
        self._sniper_wait_step: int = -1  # gale step associated with _sniper_wait_start
        self._gale_locked_direction = None  # Lock direction during gale
        self._gale_exhaustion_epoch: int = (
            0  # epoch when last gale sequence exhausted (cooldown)
        )
        # Rise/Fall specific state
        self._rf_ensemble_scorer: EnsembleScorerRF | None = None
        if config.rise_fall_use_ensemble:
            try:
                self._rf_ensemble_scorer = EnsembleScorerRF()
                logger.info("EnsembleScorerRF carregado para producao.")
            except Exception as exc:
                logger.warning("EnsembleScorerRF nao carregado: %s", exc)
        self.last_rf_entry_epoch: Optional[int] = None  # cooldown for RF
        # Multi-contract gale: split gale stake across N simultaneous contracts
        # when required stake exceeds Deriv API's per-contract limit ($1,000).
        self._gale_queue: list[float] = []  # remaining stakes to buy
        self._gale_ids: set[int] = set()  # all contract IDs in group
        self._gale_id_stakes: dict[int, float] = {}  # {cid: buy_price} for TP tracking
        self._gale_expected: int = 0  # total contracts in group
        self._gale_total_stake: float = 0.0  # nominal total (pre-split)
        self._gale_settled: dict[int, float] = {}  # {cid: profit} settled so far
        self._gale_order: Optional[PendingOrder] = None  # first order (for logging)
        self._gale_sell_requested: set[int] = set()  # contracts already sell-requested
        self._last_tick_time: float = (
            0.0  # epoch of last live tick received (for watchdog)
        )
        self._pause_log_ts: float = 0.0  # throttle "Bot pausado" log (once per 60s)
        self._balance_file = os.path.join(config.journal_dir, "balance.json")

    def _flush_balance(self, balance: float) -> None:
        """Persiste saldo atual em logs/balance.json para leitura rápida pelo dashboard."""
        try:
            tmp = self._balance_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"balance": round(balance, 2), "ts": time.time()}, f)
            os.replace(tmp, self._balance_file)
        except Exception:
            pass

    _LIVE_IND_KEYS = (
        "bb_width_percent",
        "tick_atr_percent",
        "recent_move_percent",
        "hurst_exponent",
        "tick_imbalance",
        "hawkes_intensity",
        "velocity_zscore",
        "acceleration_zscore",
        "pmi_distance_percent",
        "markov_p_up_given_up",
        "markov_p_down_given_down",
        "shannon_entropy",
        "kalman_residual_zscore",
        "bayesian_prob_up",
        "renyi_entropy",
        "fisher_information",
        "wavelet_energy_ratio",
        "cusum_score",
        "tail_dependence",
        "mi_flow",
    )

    def _write_live_indicators(self, df) -> None:
        """Write latest indicator values to JSON for real-time dashboard display."""
        if df is None or df.empty:
            return
        try:
            row = df.iloc[-1]
            data = {"timestamp": datetime.now(UTC).isoformat()}
            for k in self._LIVE_IND_KEYS:
                v = row.get(k)
                if v is not None and v == v:  # skip NaN
                    data[k] = round(float(v), 6)
                else:
                    data[k] = None
            if "avg_ret" in df.columns:
                data["avg_ret"] = round(float(df["avg_ret"].iloc[-1]), 8)
            if hasattr(self, "_last_p_loss") and self._last_p_loss is not None:
                data["p_loss"] = round(float(self._last_p_loss), 6)
            path = os.path.join(self.config.journal_dir, "live_indicators.json")
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, path)
        except Exception:
            pass

    async def send(
        self, ws: websockets.WebSocketClientProtocol, payload: dict[str, Any]
    ) -> None:
        await ws.send(json.dumps(payload))

    async def authorize(self, ws: websockets.WebSocketClientProtocol, token: str | None = None) -> None:
        auth_token = token or self.config.token
        await self.send(ws, {"authorize": auth_token})

    async def subscribe_balance(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Subscribe to real-time balance stream so manual top-ups are reflected immediately."""
        await self.send(ws, {"balance": 1, "subscribe": 1})

    async def subscribe_ticks(self, ws: websockets.WebSocketClientProtocol) -> None:
        await self.send(
            ws,
            {
                "ticks_history": self.config.symbol,
                "count": self.config.tick_count,
                "end": "latest",
                "style": "ticks",
            },
        )
        await self.send(ws, {"ticks": self.config.symbol, "subscribe": 1})

    async def request_accumulator_proposal(
        self,
        ws: websockets.WebSocketClientProtocol,
        stake: float,
        score: int,
        entry_epoch: int,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        self.pending_order = PendingOrder(stake, score, entry_epoch, metrics)
        self.journal.log_signal(
            symbol=self.config.symbol,
            contract_mode=self.config.contract_mode,
            entry_epoch=entry_epoch,
            direction="ACCU",
            score=score,
            stake=stake,
            dry_run=self.config.dry_run,
            metrics=metrics,
        )

        if self.config.dry_run:
            logger.info(
                "DRY_RUN ACCU score=%s stake=%.2f entry=%s. Nenhuma ordem enviada.",
                score,
                stake,
                entry_epoch,
            )
            self.last_accumulator_entry_epoch = entry_epoch
            self.pending_order = None
            return

        logger.info(
            "Solicitando proposta ACCU | stake=%.2f | ativo=%s | epoch=%s",
            stake,
            self.config.symbol,
            entry_epoch,
        )
        payload: dict[str, Any] = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": "ACCU",
            "currency": self.config.currency,
            "underlying_symbol": self.config.symbol,
            "growth_rate": self.config.accumulator_growth_rate,
        }

        if self.config.accumulator_use_limit_order:
            payload["limit_order"] = {
                "take_profit": round(
                    stake * self.config.accumulator_take_profit_percent / 100, 2
                )
            }

        await self.send(ws, payload)

    async def request_rise_fall_proposal(
        self,
        ws: websockets.WebSocketClientProtocol,
        stake: float,
        direction: str,
        score: int,
        entry_epoch: int,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Request a CALL or PUT proposal for Rise/Fall binary contracts."""
        self.pending_order = PendingOrder(stake, score, entry_epoch, metrics, direction)
        self.journal.log_signal(
            symbol=self.config.symbol,
            contract_mode=self.config.contract_mode,
            entry_epoch=entry_epoch,
            direction=direction,
            score=score,
            stake=stake,
            dry_run=self.config.dry_run,
            metrics=metrics,
        )

        if self.config.dry_run:
            logger.info(
                "DRY_RUN RF %s score=%s stake=%.2f entry=%s. Nenhuma ordem enviada.",
                direction,
                score,
                stake,
                entry_epoch,
            )
            self.last_rf_entry_epoch = entry_epoch
            self.pending_order = None
            return

        logger.info(
            "Solicitando proposta RF %s | stake=%.2f | ativo=%s | duration=%dt | epoch=%s",
            direction,
            stake,
            self.config.symbol,
            self.config.rise_fall_duration_ticks,
            entry_epoch,
        )
        payload: dict[str, Any] = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": direction,
            "currency": self.config.currency,
            "duration": self.config.rise_fall_duration_ticks,
            "duration_unit": "t",
            "underlying_symbol": self.config.symbol,
        }
        await self.send(ws, payload)

    async def buy_from_proposal(
        self, ws: websockets.WebSocketClientProtocol, proposal: dict[str, Any]
    ) -> None:
        if not self.pending_order:
            logger.warning("Proposta recebida sem ordem pendente.")
            return

        proposal_id = proposal.get("id")
        ask_price = proposal.get("ask_price", self.pending_order.stake)
        if not proposal_id:
            logger.error("Proposta sem id: %s", proposal)
            self.pending_order = None
            return

        # Payout filter for Rise/Fall contracts
        if self.config.contract_mode in {"rise_fall", "jump_rise_fall"}:
            payout_val = float(proposal.get("payout", 0.0))
            ask_price_val = float(ask_price)
            if payout_val > 0 and ask_price_val > 0:
                payout_pct = (payout_val - ask_price_val) / ask_price_val
                min_payout_pct = self.config.rise_fall_min_payout_pct
                if payout_pct < min_payout_pct:
                    logger.warning(
                        "⛔ [PAYOUT BLOCK] Payout proposto de %.4f%% abaixo do mínimo de %.4f%% | ask_price=%s, payout=%s",
                        payout_pct * 100,
                        min_payout_pct * 100,
                        ask_price_val,
                        payout_val
                    )
                    self.pending_order = None
                    return

        self.waiting_for_result = True
        self._waiting_since = time.monotonic()
        direction = getattr(self.pending_order, "direction", "ACCU")
        logger.info(
            "Comprando %s | proposal_id=%s | price=%s",
            direction,
            proposal_id,
            ask_price,
        )
        await self.send(ws, {"buy": proposal_id, "price": ask_price})

    async def subscribe_contract(
        self, ws: websockets.WebSocketClientProtocol, contract_id: int
    ) -> None:
        await self.send(
            ws,
            {
                "proposal_open_contract": 1,
                "contract_id": contract_id,
                "subscribe": 1,
            },
        )

    async def sell_contract(
        self,
        ws: websockets.WebSocketClientProtocol,
        contract_id: int,
        price: float = 0.0,
    ) -> None:
        if self.accumulator_sell_requested:
            return

        self.accumulator_sell_requested = True
        logger.info("Vendendo ACCU id=%s price=%s", contract_id, price)
        await self.send(ws, {"sell": contract_id, "price": round(float(price), 2)})

    def _append_tick(self, tick: dict[str, Any]) -> bool:
        epoch = int(tick["epoch"])
        normalized = {
            "epoch": epoch,
            "quote": tick["quote"],
        }

        if self.tick_buffer and int(self.tick_buffer[-1]["epoch"]) == epoch:
            self.tick_buffer[-1] = normalized
            return False

        self.tick_buffer.append(normalized)
        return True

    async def evaluate_tick(
        self, ws: websockets.WebSocketClientProtocol, tick_epoch: int
    ) -> None:
        _is_rf_like = self.config.contract_mode in {"rise_fall", "jump_rise_fall"}

        if self.waiting_for_result:
            stuck_sec = time.monotonic() - self._waiting_since
            if stuck_sec > 30:
                logger.warning(
                    "waiting_for_result timeout (%.0fs sem resposta) — resetando estado e retomando operacoes.",
                    stuck_sec,
                )
                # CORREÇÃO: timeout = trade perdido. Tratar como LOSS para manter contabilidade correta.
                if self.risk and self.pending_order:
                    buy_price = self.pending_order.stake
                    logger.warning(
                        "⚠️ Trade timeout contabilizado como LOSS de %.2f (contrato pode ter liquidado sem resposta)",
                        buy_price,
                    )
                    self.risk.update(profit=-buy_price, buy_price=buy_price)
                    self._flush_balance(
                        self.risk.balance
                    )  # atualiza dashboard imediatamente
                    logger.info(self.risk.stats())
                elif self.risk:
                    # Sem pending_order mas em gale — reseta martingale para evitar step fantasma
                    if self.risk.use_martingale and self.risk.martingale_step > 0:
                        logger.warning(
                            "⚠️ Timeout sem pending_order durante gale %d/%d — resetando martingale (perdas=%.2f absorvidas)",
                            self.risk.martingale_step,
                            self.risk.martingale_max_gales,
                            self.risk.martingale_accumulated_loss,
                        )
                        self.risk.martingale_step = 0
                        self.risk.martingale_accumulated_loss = 0.0
                        self.risk.martingale_base_stake = 0.0
                        self.risk._save_state()
                self._reset_gale_state()
                self.waiting_for_result = False
                self.pending_order = None
                self.current_contract_id = None
                self.accumulator_sell_requested = False
            else:
                _mode_lbl = "RF" if _is_rf_like else "ACCU"
                logger.info("Aguardando resultado da operacao %s anterior.", _mode_lbl)
                return

        if self.pending_order:
            _mode_lbl = "RF" if _is_rf_like else "ACCU"
            logger.info("Aguardando proposta/compra %s pendente.", _mode_lbl)
            return

        if not self.risk:
            logger.warning("RiskManager ainda nao inicializado.")
            return

        tick_dt = datetime.fromtimestamp(tick_epoch, UTC)
        tick_hour = tick_dt.hour
        # BLOCK_HOURS_ENABLED=false desativa completamente o bloqueio de hora
        _block_hours_enabled = (
            os.getenv("BLOCK_HOURS_ENABLED", "true").strip().lower() != "false"
        )
        if _block_hours_enabled and tick_hour in self.config.blocked_utc_hours:
            logger.debug("Hora UTC bloqueada: %s", tick_hour)
            return

        # Block weekends: Friday 21:00 UTC → Sunday 21:00 UTC
        if getattr(self.risk, "block_weekends", self.config.block_weekends):
            dow = tick_dt.weekday()  # 0=Mon … 4=Fri, 5=Sat, 6=Sun
            is_blocked_period = (
                dow == 6  # all Sunday
                or dow == 5  # all Saturday
                or (dow == 4 and tick_hour >= 21)  # Friday from 21:00 UTC
            )
            if is_blocked_period:
                logger.info(
                    "Fim de semana bloqueado (BLOCK_WEEKENDS=true): dow=%s hour=%s",
                    dow,
                    tick_hour,
                )
                return

        # Compute indicators on EVERY tick for live dashboard
        # (before cooldown/risk checks so indicators stay fresh)
        _tick_snapshot = list(self.tick_buffer)
        df = await asyncio.to_thread(
            calculate_tick_indicators,
            _tick_snapshot,
            config=self.config.accumulator_strategy_config,
        )
        self._write_live_indicators(df)

        # Accumulator cooldown — only applies to accumulator mode
        if not _is_rf_like and self.last_accumulator_entry_epoch is not None:
            ticks_since_entry = tick_epoch - self.last_accumulator_entry_epoch
            if ticks_since_entry <= self.config.accumulator_cooldown_ticks:
                logger.info(
                    "Cooldown ACCU ativo: %s tick(s) desde a ultima entrada; minimo=%s.",
                    ticks_since_entry,
                    self.config.accumulator_cooldown_ticks + 1,
                )
                return

        # DYNAMIC COOLDOWN BYPASS (FRANKENSTEIN EXCLUSIVE):
        # Se o bot estiver em cooldown de sessão (cooldown_until > 0), mas as condições de calmaria 
        # forem excepcionais, saímos do cooldown antecipadamente!
        if getattr(self.risk, "cooldown_until", 0.0) > 0 and df is not None and not df.empty:
            prices = [t["quote"] for t in _tick_snapshot]
            lookback = self.config.calm_accu_lookback
            if len(prices) >= lookback + 1:
                recent_p = prices[-(lookback + 1) :]
                abs_returns = [abs(recent_p[i] / recent_p[i - 1] - 1) for i in range(1, len(recent_p))]
                avg_abs_ret = sum(abs_returns) / len(abs_returns)
                
                _last = df.iloc[-1]
                _cusum = float(_last.get("cusum_score", 0.0) or 0.0)
                _hurst = float(_last.get("hurst_exponent", 0.5) or 0.5)
                
                # Critério de Calmaria Extrema:
                # 1. Volatilidade média abaixo do limiar de calmaria
                # 2. CUSUM abaixo de 3.0 (sem tendência repentina de spike)
                # 3. Hurst acima de 0.45 (sem ruído anti-trend agressivo)
                if (
                    avg_abs_ret < self.config.calm_accu_threshold
                    and _cusum < 3.0
                    and _hurst > 0.45
                ):
                    logger.warning(
                        "⚡ DYNAMIC CALM RESUME: mercado calmo detectado (volatilidade=%.2e, CUSUM=%.2f, H=%.3f) — encerrando cooldown antecipadamente!",
                        avg_abs_ret,
                        _cusum,
                        _hurst,
                    )
                    self.risk.reset_cooldown_early()

        if not self.risk.can_trade():
            now = time.monotonic()
            if now - self._pause_log_ts >= 60:
                logger.warning("Bot pausado por regra de risco.")
                self._pause_log_ts = now
            return

        # ---- Jump Rise/Fall mode (JD10, JD25, JD50, JD75, JD100) ----
        if self.config.contract_mode == "jump_rise_fall":
            if self.last_rf_entry_epoch is not None:
                ticks_since_rf = tick_epoch - self.last_rf_entry_epoch
                if ticks_since_rf <= self.config.rise_fall_cooldown_ticks:
                    logger.debug(
                        "Cooldown JumpRF ativo: %s tick(s) desde ultima entrada; minimo=%s.",
                        ticks_since_rf,
                        self.config.rise_fall_cooldown_ticks + 1,
                    )
                    return

            _jm_config = JumpMomentumConfig(
                mom_lookback=5,
                mom_horizon=self.config.rise_fall_duration_ticks,
                ema_fast=5,
                ema_slow=20,
                rev_lookback=7,
                min_score=self.config.rise_fall_min_votes,
                min_confidence=self.config.rise_fall_min_confidence,
                min_ticks=30,
                quality_gate_enabled=self.config.rise_fall_quality_gate,
                qg_min_abs_imbalance=self.config.rise_fall_qg_min_abs_imbalance,
                qg_bayes_strong=self.config.rise_fall_qg_bayes_strong,
                qg_hurst_max=self.config.rise_fall_qg_hurst_max,
            )
            signal, score, confidence = await asyncio.to_thread(
                generate_jump_momentum_signal,
                _tick_snapshot,
                config=_jm_config,
                df=df,
            )

            # --- GALE DIRECTION LOCK + CONFIDENCE FILTER ---
            _in_gale = (
                getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
            )

            # Cooldown after gale exhaustion: wait 30 ticks before new sequence
            if not _in_gale and self._gale_exhaustion_epoch > 0:
                ticks_since_exhaust = tick_epoch - self._gale_exhaustion_epoch
                if ticks_since_exhaust < 30:
                    logger.debug(
                        "Gale cooldown: %d/30 ticks desde exaustão — aguardando.",
                        ticks_since_exhaust,
                    )
                    return
                else:
                    self._gale_exhaustion_epoch = 0

            if signal not in {"CALL", "PUT"}:
                if _in_gale:
                    logger.debug(
                        "GALE %d/%d: sem sinal JumpRF — aguardando.",
                        self.risk.martingale_step,
                        self.risk.martingale_max_gales,
                    )
                else:
                    logger.debug("Sem setup JumpRF no tick %s.", tick_epoch)
                return

            if _in_gale:
                # --- REGIME GUARD + PROGRESSIVE VOTE INTELLIGENCE ---
                # Quanto mais fundo no gale, mais rigoroso o filtro.
                # G1-3: warm-up, G4-6: sniper, G7-9: deep, G10-15: ultra.
                _step = self.risk.martingale_step

                # ── SNIPER TIMEOUT: abandona gale se bloqueado por muito tempo sem conseguir entrar ──
                if _step >= 4:
                    _sniper_max_secs = self.config.martingale_sniper_max_wait_secs
                    if _sniper_max_secs > 0:
                        if self._sniper_wait_step != _step:
                            # Novo nível de gale — reinicia o contador
                            self._sniper_wait_start = time.monotonic()
                            self._sniper_wait_step = _step
                        elif (
                            time.monotonic() - self._sniper_wait_start
                        ) >= _sniper_max_secs:
                            _waited = time.monotonic() - self._sniper_wait_start
                            logger.warning(
                                "⏰ SNIPER TIMEOUT G%d: aguardando há %.0fs (limite=%ds) — absorvendo perdas=%.2f e resetando",
                                _step,
                                _waited,
                                _sniper_max_secs,
                                self.risk.martingale_accumulated_loss,
                            )
                            self._sniper_wait_start = 0.0
                            self._sniper_wait_step = -1
                            self.risk.abandon_gale()
                            return

                def _ind(name: str, default: float = 0.0) -> float:
                    if name in df.columns and len(df) > 0:
                        v = df[name].iloc[-1]
                        try:
                            f = float(v if v is not None else default)
                        except (TypeError, ValueError):
                            f = default
                        return default if f != f else f
                    return default

                def _ind_valid(name: str) -> bool:
                    """True if indicator column exists and last value is not NaN."""
                    if name not in df.columns or len(df) == 0:
                        return False
                    v = df[name].iloc[-1]
                    if v is None:
                        return False
                    try:
                        f = float(v)
                        return f == f  # False for NaN
                    except (TypeError, ValueError):
                        return False

                _cusum = _ind("cusum_score")
                _hurst = _ind("hurst_exponent", 0.5)
                _bayes = _ind("bayesian_prob_up", 0.5)

                # ── REGIME GUARD: thresholds progressivos por profundidade ──
                # Thresholds calibrados com dados reais do instrumento:
                #   cusum típico: 3-5, hurst: 0.35-0.45, bayes: 0.50-0.60
                if _step >= 4:
                    # Cusum: 6.0 no G4, -0.25/step, piso 3.0 (safety net p/ regime extremo)
                    _max_cusum = max(6.0 - (_step - 4) * 0.25, 3.0)
                    if _ind_valid("cusum_score") and _cusum > _max_cusum:
                        logger.info(
                            "🎯 SNIPER G%d: cusum=%.2f > %.2f — aguardando regime calmo.",
                            _step,
                            _cusum,
                            _max_cusum,
                        )
                        return
                    # Hurst: 0.60 no G4, -0.01/step, piso 0.48
                    _max_hurst = max(0.60 - (_step - 4) * 0.01, 0.48)
                    if _ind_valid("hurst_exponent") and _hurst > _max_hurst:
                        logger.info(
                            "🎯 SNIPER G%d: hurst=%.3f > %.3f — aguardando mercado lateral.",
                            _step,
                            _hurst,
                            _max_hurst,
                        )
                        return
                    # Bayesian: contra-direction safety — só bloqueia se bayes
                    # contradiz FORTEMENTE o sinal (não exige confirmação).
                    # G4: bloqueia CALL se bayes < 0.30, PUT se bayes > 0.70
                    _bayes_contra = max(0.30 - (_step - 4) * 0.01, 0.20)
                    if _ind_valid("bayesian_prob_up"):
                        _bayes_block = (
                            _bayes < _bayes_contra and signal == "CALL"
                        ) or (_bayes > (1.0 - _bayes_contra) and signal == "PUT")
                        if _bayes_block:
                            logger.info(
                                "🎯 SNIPER G%d: bayes=%.3f contra %s (limite %.2f/%.2f) — aguardando.",
                                _step,
                                _bayes,
                                signal,
                                _bayes_contra,
                                1.0 - _bayes_contra,
                            )
                            return
                elif _step >= 3:
                    if _ind_valid("cusum_score") and _cusum > 7.0:
                        logger.info(
                            "GALE %d/%d: cusum=%.2f > 7.0 — aguardando estabilizar.",
                            _step,
                            self.risk.martingale_max_gales,
                            _cusum,
                        )
                        return
                    if _ind_valid("hurst_exponent") and _hurst > 0.65:
                        logger.info(
                            "GALE %d/%d: hurst=%.3f > 0.65 — aguardando.",
                            _step,
                            self.risk.martingale_max_gales,
                            _hurst,
                        )
                        return

                # ── DEEP GALE G8+: validadores extras de qualidade ──
                if _step >= 8:
                    _wavelet = _ind("wavelet_energy_ratio", 0.0)
                    _mi = _ind("mi_flow", 0.0)
                    # Wavelet SNR: sinal limpo — skip se indicador retorna 0 (inativo)
                    if (
                        _ind_valid("wavelet_energy_ratio")
                        and _wavelet > 0.0
                        and _wavelet < 0.30
                    ):
                        logger.info(
                            "🔬 DEEP G%d: wavelet=%.3f < 0.30 — sinal ruidoso, aguardando.",
                            _step,
                            _wavelet,
                        )
                        return
                    # MI: estrutura previsível (>0.01)
                    if _ind_valid("mi_flow") and _mi < 0.01:
                        logger.info(
                            "🔬 DEEP G%d: mi_flow=%.4f < 0.01 — sem estrutura, aguardando.",
                            _step,
                            _mi,
                        )
                        return

                # ── ULTRA GALE G10+: condições quase perfeitas ──
                if _step >= 10:
                    _shannon = _ind("shannon_entropy", 1.0)
                    _fisher = _ind("fisher_information", 0.0)
                    # Shannon baixo = retornos concentrados = previsível
                    if _ind_valid("shannon_entropy") and _shannon > 0.95:
                        logger.info(
                            "🔬 ULTRA G%d: shannon=%.3f > 0.95 — mercado disperso, aguardando.",
                            _step,
                            _shannon,
                        )
                        return
                    # Fisher alto = distribuição apertada = confiável
                    if _ind_valid("fisher_information") and _fisher < 0.01:
                        logger.info(
                            "🔬 ULTRA G%d: fisher=%.4f < 0.01 — distribuição frouxa, aguardando.",
                            _step,
                            _fisher,
                        )
                        return

                # ── EXTREME GALE G12+: Lyapunov estável ──
                if _step >= 12:
                    _lyap = _ind("lyapunov_exponent", 0.0)
                    if _ind_valid("lyapunov_exponent") and _lyap > 1.0:
                        logger.info(
                            "🔬 EXTREME G%d: lyapunov=%.3f > 1.00 — mercado caótico, aguardando.",
                            _step,
                            _lyap,
                        )
                        return

                # ── VOTOS + CONFIANÇA: escalamento progressivo ──
                # Confiança: 0.62 + step*0.02, teto 0.95 (de 21 validadores)
                _gale_min_conf = min(0.62 + _step * 0.02, 0.95)
                # Score: base + 1 + step//2, teto base+8
                _base = self.config.rise_fall_min_votes
                _gale_min_score = min(_base + 1 + _step // 2, _base + 8)

                if confidence is not None and confidence < _gale_min_conf:
                    logger.debug(
                        "GALE %d/%d: conf=%.0f%% < min=%.0f%% — aguardando sinal mais forte.",
                        _step,
                        self.risk.martingale_max_gales,
                        confidence * 100,
                        _gale_min_conf * 100,
                    )
                    return
                if score < _gale_min_score:
                    logger.debug(
                        "GALE %d/%d: score=%d < min=%d — aguardando sinal mais forte.",
                        _step,
                        self.risk.martingale_max_gales,
                        score,
                        _gale_min_score,
                    )
                    return

                # ── DIRECTION LOCK: inversão precisa conf >= lock threshold ──
                # G1-6: 80%, G7-9: 85%, G10+: 90% (mais fundo = mais difícil inverter)
                _invert_min_conf = 0.80 if _step < 7 else (0.85 if _step < 10 else 0.90)
                if (
                    self._gale_locked_direction is not None
                    and signal != self._gale_locked_direction
                ):
                    if confidence is not None and confidence >= _invert_min_conf:
                        logger.info(
                            "GALE %d/%d: invertendo %s→%s (conf=%.0f%% >= %.0f%%)",
                            self.risk.martingale_step,
                            self.risk.martingale_max_gales,
                            self._gale_locked_direction,
                            signal,
                            confidence * 100,
                            _invert_min_conf * 100,
                        )
                        self._gale_locked_direction = signal
                    else:
                        logger.debug(
                            "GALE %d/%d: sinal %s != lock %s e conf=%.0f%% < %.0f%% — ignorando.",
                            self.risk.martingale_step,
                            self.risk.martingale_max_gales,
                            signal,
                            self._gale_locked_direction,
                            (confidence or 0) * 100,
                            _invert_min_conf * 100,
                        )
                        return
                elif self._gale_locked_direction is None:
                    self._gale_locked_direction = signal
                    logger.info(
                        "GALE %d/%d: travando direção=%s para sequência de gale.",
                        self.risk.martingale_step,
                        self.risk.martingale_max_gales,
                        signal,
                    )
                signal = self._gale_locked_direction
            else:
                # Fora de gale: limpar lock
                if self._gale_locked_direction is not None:
                    self._gale_locked_direction = None

            stake = self.risk.get_stake()
            if (
                getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
            ):
                raw_gale = self.risk.get_gale_raw_stake()
                if raw_gale > stake:
                    logger.info(
                        "GALE JumpRF cap: stake_full=%.2f → capped=%.2f (MAX_STAKE=%.2f)",
                        raw_gale,
                        stake,
                        self.config.max_stake,
                    )

            if self._gale_wait_ticks > 0:
                self._gale_wait_ticks = 0

            metrics = self._last_tick_metrics(df)
            if confidence is not None:
                metrics["jump_confidence"] = round(confidence, 4)
            _mode = (
                f"GALE {self.risk.martingale_step}/{self.risk.martingale_max_gales}"
                if getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
                else f"SOROS {self.risk.soros_step}/{self.risk.soros_max_steps}"
                if getattr(self.risk, "use_soros", False) and self.risk.soros_step > 0
                else "NORMAL"
            )
            logger.info(
                "Setup JumpRF %s detectado: score=%s stake=%.2f conf=%s modo=%s",
                signal,
                score,
                stake,
                f"{confidence:.4f}" if confidence is not None else "N/A",
                _mode,
            )
            await self.request_rise_fall_proposal(
                ws, stake, signal, score, tick_epoch, metrics=metrics
            )
            return

        # ---- Rise/Fall mode ----
        if self.config.contract_mode == "rise_fall":
            if self.last_rf_entry_epoch is not None:
                ticks_since_rf = tick_epoch - self.last_rf_entry_epoch
                if ticks_since_rf <= self.config.rise_fall_cooldown_ticks:
                    logger.debug(
                        "Cooldown RF ativo: %s tick(s) desde ultima entrada; minimo=%s.",
                        ticks_since_rf,
                        self.config.rise_fall_cooldown_ticks + 1,
                    )
                    return

            signal, score, p_dir = await asyncio.to_thread(
                generate_rise_fall_signal,
                df,
                config=self.config.rise_fall_strategy_config,
                ensemble_scorer=self._rf_ensemble_scorer,
            )

            if signal not in {"CALL", "PUT"}:
                logger.debug("Sem setup RF no tick %s.", tick_epoch)
                return

            stake = self.risk.get_stake()
            if (
                getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
            ):
                raw_gale = self.risk.get_gale_raw_stake()
                if raw_gale > stake:
                    logger.info(
                        "GALE RF cap: stake_full=%.2f → capped=%.2f (MAX_STAKE=%.2f)",
                        raw_gale,
                        stake,
                        self.config.max_stake,
                    )

            if self._gale_wait_ticks > 0:
                self._gale_wait_ticks = 0

            metrics = self._last_tick_metrics(df)
            _mode = (
                f"GALE {self.risk.martingale_step}/{self.risk.martingale_max_gales}"
                if getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
                else f"SOROS {self.risk.soros_step}/{self.risk.soros_max_steps}"
                if getattr(self.risk, "use_soros", False) and self.risk.soros_step > 0
                else "NORMAL"
            )
            logger.info(
                "Setup RF %s detectado: score=%s stake=%.2f p_dir=%s modo=%s",
                signal,
                score,
                stake,
                f"{p_dir:.4f}" if p_dir is not None else "N/A",
                _mode,
            )
            await self.request_rise_fall_proposal(
                ws, stake, signal, score, tick_epoch, metrics=metrics
            )
            return

        # ---- Calm ACCU mode (BOOM1000 calm-entry) ----
        if self.config.contract_mode == "calm_accu":
            prices = [t["quote"] for t in _tick_snapshot]
            signal, score, p_loss = generate_calm_accu_signal(
                prices,
                threshold=self.config.calm_accu_threshold,
                lookback=self.config.calm_accu_lookback,
                df=df,
                config=self.config.accumulator_strategy_config,
                ensemble_scorer=self._ensemble_scorer,
            )
            self._last_p_loss = p_loss
            if signal != "ACCU":
                logger.debug("Sem setup CALM ACCU no tick %s.", tick_epoch)
                return

            # --- SUPER-FRANKENSTEIN: Dynamic Regime Switch ---
            regime_tp = float(os.getenv("ACCUMULATOR_TAKE_PROFIT_PERCENT", "30.0"))
            regime_hold = int(os.getenv("ACCUMULATOR_MAX_HOLD_TICKS", "9"))
            
            is_absolute_calm = False
            is_medium_calm = False
            _cusum = 0.0
            _hurst = 0.5
            _shannon = 0.0
            _kalman = 0.0
            
            recent = prices[-(self.config.calm_accu_lookback + 1) :]
            abs_returns = [abs(recent[i] / recent[i - 1] - 1) for i in range(1, len(recent))]
            avg_abs_ret = sum(abs_returns) / len(abs_returns) if abs_returns else 0.0
            
            if df is not None and not df.empty:
                _last = df.iloc[-1]
                _cusum = float(_last.get("cusum_score", 0.0) or 0.0)
                _hurst = float(_last.get("hurst_exponent", 0.5) or 0.5)
                _shannon = float(_last.get("shannon_entropy", 0.0) or 0.0)
                _kalman = float(_last.get("kalman_residual_zscore", 0.0) or 0.0)
                
                # Calmaria Extrema (Regime A) Check:
                _pass_a_xgb = (p_loss is None or p_loss < 0.22)
                
                median_vol = get_symbol_median_volatility(self.config.symbol)
                if (
                    avg_abs_ret < 1.0 * median_vol
                    and _cusum < 2.5
                    and _hurst > 0.48
                    and _shannon > 0.85
                    and abs(_kalman) < 1.5
                    and _pass_a_xgb
                ):
                    is_absolute_calm = True
                
                # Calmaria Moderada (Regime B+) Check:
                _pass_b_plus_xgb = (p_loss is None or p_loss < 0.26)
                if (
                    avg_abs_ret < 2.2 * median_vol
                    and _cusum < 4.0
                    and _hurst > 0.45
                    and _pass_b_plus_xgb
                ):
                    is_medium_calm = True
            
            _in_gale = getattr(self.risk, "use_martingale", False) and self.risk.martingale_step > 0
            
            if _in_gale:
                # DYNAMIC GALE BYPASS: se a IA prever baixíssimo risco (P(LOSS) < 15%), ignoramos a calmaria extrema e disparamos!
                _xgb_bypass = (p_loss is not None and p_loss < float(os.getenv("PCS_XGB_BYPASS_LIMIT", "0.15")))
                if not is_absolute_calm and not _xgb_bypass:
                    logger.info(
                        "⏳ GALE STANDBY: aguardando calmaria extrema ou IA Bypass para executar Gale %d (vol=%.2e, CUSUM=%.2f, H=%.3f, P(LOSS)=%s)",
                        self.risk.martingale_step,
                        avg_abs_ret,
                        _cusum,
                        _hurst,
                        f"{p_loss:.4f}" if p_loss is not None else "N/A",
                    )
                    return
                
                # Calmaria extrema ou IA Bypass detectada: executa o Gale no Regime A (30% TP)
                object.__setattr__(self.config, 'accumulator_take_profit_percent', regime_tp)
                object.__setattr__(self.config, 'accumulator_max_hold_ticks', regime_hold)
                self.risk.use_soros = False
                if _xgb_bypass:
                    logger.warning(
                        "🔥 GALE BYPASS FIRE: Executando Gale %d via IA (P(LOSS)=%.4f < %.2f) em mercado volátil!",
                        self.risk.martingale_step,
                        p_loss,
                        float(os.getenv("PCS_XGB_BYPASS_LIMIT", "0.15")),
                    )
                else:
                    logger.info(
                        "🔥 GALE FIRE: Executando Gale %d no Regime A (30%% TP, 9 Ticks) na calmaria extrema",
                        self.risk.martingale_step,
                    )
            else:
                # Modo normal (sem Gale): seleciona regime baseado na calmaria (A, B+, ou B-)
                if is_absolute_calm:
                    # Regime A: Sniper Pro 30% TP com Soros ATIVO
                    object.__setattr__(self.config, 'accumulator_take_profit_percent', regime_tp)
                    object.__setattr__(self.config, 'accumulator_max_hold_ticks', regime_hold)
                    self.risk.use_soros = True
                    logger.info(
                        "🔥 PEGASUS CONGLOMERATE: REGIME A (Sniper Pro 30%% TP, 9 Ticks) na calmaria extrema (vol=%.2e, CUSUM=%.2f, H=%.3f) — Soros ATIVO",
                        avg_abs_ret,
                        _cusum,
                        _hurst,
                    )
                elif is_medium_calm:
                    # Regime B+: Medium Harvester 9% TP com 3 Ticks e Soros DESATIVADO
                    regime_b_plus_tp = float(os.getenv("PCS_REGIME_B_PLUS_TP", "9.0"))
                    regime_b_plus_hold = int(os.getenv("PCS_REGIME_B_PLUS_HOLD", "3"))
                    object.__setattr__(self.config, 'accumulator_take_profit_percent', regime_b_plus_tp)
                    object.__setattr__(self.config, 'accumulator_max_hold_ticks', regime_b_plus_hold)
                    self.risk.use_soros = False
                    logger.info(
                        "🌾 PEGASUS CONGLOMERATE: REGIME B+ (Medium Harvester %.1f%% TP, %d Ticks) na calmaria moderada (vol=%.2e, CUSUM=%.2f, H=%.3f) — Soros DESATIVADO",
                        self.config.accumulator_take_profit_percent,
                        self.config.accumulator_max_hold_ticks,
                        avg_abs_ret,
                        _cusum,
                        _hurst,
                    )
                else:
                    logger.info(
                        "🛡️ PEGASUS CONGLOMERATE: REGIME B- (Defensive Harvester) DETECTADO — Ignorando entrada (evita prejuízos com InvalidtoSell / spread).",
                    )
                    return

            stake = self.risk.get_stake()

            if (
                getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
            ):
                raw_gale = self.risk.get_gale_raw_stake()
                if raw_gale > stake:
                    logger.info(
                        "GALE cap: stake_full=%.2f → capped=%.2f (MAX_STAKE=%.2f) — recuperacao parcial",
                        raw_gale,
                        stake,
                        self.config.max_stake,
                    )
            if self._gale_wait_ticks > 0:
                logger.info(
                    "✅ Sinal seguro encontrado após %d ticks de espera",
                    self._gale_wait_ticks,
                )
                self._gale_wait_ticks = 0
                self._gale_wait_log_ts = 0.0
            metrics = self._last_tick_metrics(df)

            # ── QUALITY GATE: filtros de indicadores no momento da entrada ──────
            # Baseado em análise de dados reais: cusum 5.5-7 e H < 0.45 destroem P&L.
            # Bloqueia entradas na zona de perigo sem parar o bot.
            if df is not None and not df.empty:
                _last = df.iloc[-1]
                _cusum = float(_last.get("cusum_score", 0.0) or 0.0)
                _hurst = float(_last.get("hurst_exponent", 0.5) or 0.5)
                _max_cusum = self.config.calm_accu_max_entry_cusum
                _min_hurst = self.config.accumulator_min_hurst_exponent
                if _max_cusum > 0 and _cusum > _max_cusum:
                    logger.info(
                        "CALM ACCU QUALITY GATE: cusum=%.2f > max=%.2f — skip (zona de perigo)",
                        _cusum,
                        _max_cusum,
                    )
                    return
                if _min_hurst > 0 and _hurst < _min_hurst:
                    logger.info(
                        "CALM ACCU QUALITY GATE: H=%.3f < min=%.3f — skip (mercado anti-trend)",
                        _hurst,
                        _min_hurst,
                    )
                    return
            # ── fim QUALITY GATE ─────────────────────────────────────────────────

            _mode = (
                f"GALE {self.risk.martingale_step}/{self.risk.martingale_max_gales}"
                if getattr(self.risk, "use_martingale", False)
                and self.risk.martingale_step > 0
                else f"SOROS {self.risk.soros_step}/{self.risk.soros_max_steps}"
                if getattr(self.risk, "use_soros", False) and self.risk.soros_step > 0
                else "NORMAL"
            )
            logger.info(
                "Setup CALM ACCU detectado: score=%s stake=%.2f modo=%s avg_ret<%.2e",
                score,
                stake,
                _mode,
                self.config.calm_accu_threshold,
            )
            await self.request_accumulator_proposal(
                ws, stake, score, tick_epoch, metrics=metrics
            )
            return

        # ---- Accumulator mode (default) ----
        signal, score, p_loss = await asyncio.to_thread(
            generate_accumulator_signal,
            df,
            config=self.config.accumulator_strategy_config,
            ensemble_scorer=self._ensemble_scorer,
        )

        if signal != "ACCU":
            logger.debug("Sem setup ACCU no tick %s.", tick_epoch)
            return

        stake = self.risk.get_stake()
        if (
            getattr(self.risk, "use_martingale", False)
            and self.risk.martingale_step > 0
        ):
            raw_gale = self.risk.get_gale_raw_stake()
            if raw_gale > stake:
                logger.info(
                    "GALE cap: stake_full=%.2f → capped=%.2f (MAX_STAKE=%.2f) — recuperacao parcial",
                    raw_gale,
                    stake,
                    self.config.max_stake,
                )
            # Proteção: não entrar no último gale com P(LOSS) alto — AGUARDA sinal seguro
            max_ploss = self.config.martingale_last_gale_max_ploss
            if (
                max_ploss > 0
                and self.risk.martingale_step == self.risk.martingale_max_gales
                and p_loss is not None
                and p_loss >= max_ploss
            ):
                self._gale_wait_ticks += 1
                max_wait = self.config.martingale_last_gale_max_wait_ticks
                if max_wait > 0 and self._gale_wait_ticks >= max_wait:
                    logger.error(
                        "⛔ Gale %d/%d TIMEOUT após %d ticks aguardando P(LOSS) < %.0f%% — absorvendo perdas=%.2f",
                        self.risk.martingale_step,
                        self.risk.martingale_max_gales,
                        self._gale_wait_ticks,
                        max_ploss * 100,
                        self.risk.martingale_accumulated_loss,
                    )
                    self._gale_wait_ticks = 0
                    self._gale_wait_log_ts = 0.0
                    self.risk.abandon_gale()
                    return
                _now = time.monotonic()
                if _now - self._gale_wait_log_ts >= 10.0:
                    wait_info = (
                        f"{self._gale_wait_ticks}/{max_wait}t"
                        if max_wait > 0
                        else f"{self._gale_wait_ticks}t"
                    )
                    logger.warning(
                        "⏳ Gale %d/%d aguardando P(LOSS) < %.0f%% — atual=%.1f%% (%s) (stake salva=%.2f)",
                        self.risk.martingale_step,
                        self.risk.martingale_max_gales,
                        max_ploss * 100,
                        p_loss * 100,
                        wait_info,
                        stake,
                    )
                    self._gale_wait_log_ts = _now
                return  # mantém estado do gale, aguarda próximo tick seguro
        if self._gale_wait_ticks > 0:
            logger.info(
                "✅ Sinal seguro encontrado após %d ticks de espera",
                self._gale_wait_ticks,
            )
            self._gale_wait_ticks = 0
            self._gale_wait_log_ts = 0.0
        metrics = self._last_tick_metrics(df)
        _mode = (
            f"GALE {self.risk.martingale_step}/{self.risk.martingale_max_gales}"
            if getattr(self.risk, "use_martingale", False)
            and self.risk.martingale_step > 0
            else f"SOROS {self.risk.soros_step}/{self.risk.soros_max_steps}"
            if getattr(self.risk, "use_soros", False) and self.risk.soros_step > 0
            else "NORMAL"
        )
        logger.info(
            "Setup ACCU detectado: score=%s stake=%.2f p_loss=%s modo=%s",
            score,
            stake,
            f"{p_loss:.4f}" if p_loss is not None else "N/A",
            _mode,
        )
        await self.request_accumulator_proposal(
            ws, stake, score, tick_epoch, metrics=metrics
        )

    @staticmethod
    def _last_tick_metrics(df: Any) -> dict[str, float]:
        if df.empty:
            return {}

        last = df.iloc[-1]
        metrics: dict[str, float] = {}
        for name in (
            "bb_width_percent",
            "tick_atr_percent",
            "recent_move_percent",
            "hurst_exponent",
            "tick_imbalance",
            "hawkes_intensity",
            "velocity_zscore",
            "acceleration_zscore",
            "pmi_distance_percent",
            "markov_p_up_given_up",
            "markov_p_down_given_down",
            "shannon_entropy",
            "kalman_residual_zscore",
            # Advanced calculus indicators
            "jerk_zscore",
            "curvature_zscore",
            "integral_momentum_div",
            "derivative_energy",
            "trend_exhaustion",
            "return_zscore",
            "lyapunov_exponent",
            # Advanced intelligence filters
            "bayesian_prob_up",
            "renyi_entropy",
            "fisher_information",
            "wavelet_energy_ratio",
            "cusum_score",
            "tail_dependence",
            "mi_flow",
        ):
            try:
                value = float(last.get(name))
            except (TypeError, ValueError):
                continue
            if value == value:
                metrics[name] = value
        return metrics

    def _reset_gale_state(self) -> None:
        """Clear all multi-contract gale tracking state."""
        self._gale_queue = []
        self._gale_ids = set()
        self._gale_id_stakes = {}
        self._gale_expected = 0
        self._gale_total_stake = 0.0
        self._gale_settled = {}
        self._gale_order = None
        self._gale_sell_requested = set()

    async def _request_gale_proposal(
        self, ws: websockets.WebSocketClientProtocol, stake: float
    ) -> None:
        """Send a proposal for the next contract in a multi-gale split sequence."""
        payload: dict[str, Any] = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": "ACCU",
            "currency": self.config.currency,
            "underlying_symbol": self.config.symbol,
            "growth_rate": self.config.accumulator_growth_rate,
        }
        if self.config.accumulator_use_limit_order:
            payload["limit_order"] = {
                "take_profit": round(
                    stake * self.config.accumulator_take_profit_percent / 100, 2
                )
            }
        logger.info(
            "GALE seq: proposta stake=%.2f (%d restantes na fila apos este)",
            stake,
            len(self._gale_queue),
        )
        await self.send(ws, payload)

    async def initialize_risk_and_subscriptions(
        self,
        ws: websockets.WebSocketClientProtocol,
        balance: float,
        loginid: str,
        is_demo: bool,
    ) -> None:
        """Inicializa RiskManager, assina canais e reconcilia contratos abertos."""
        if self.config.account_mode == "demo" and not is_demo:
            raise FatalBotError(
                f"ACCOUNT_MODE=demo, mas a API autorizou loginid={loginid}. Use token demo VRTC ou mude a config."
            )
        if self.config.account_mode == "real" and is_demo:
            raise FatalBotError(
                f"ACCOUNT_MODE=real, mas a API autorizou loginid={loginid}. Use token real ou mude a config."
            )
        if (
            not self.config.dry_run
            and not is_demo
            and not self.config.allow_real_trading
        ):
            raise FatalBotError(
                "Conta real detectada. Defina ALLOW_REAL_TRADING=true somente depois dos testes em demo."
            )

        mode = "DRY_RUN" if self.config.dry_run else "LIVE"
        account_type = "demo" if is_demo else "real/indefinida"
        # max_loss_day: use percentage of current balance if MAX_LOSS_DAY_PCT is set
        if self.config.max_loss_day_pct > 0:
            max_loss_day = round(balance * self.config.max_loss_day_pct, 2)
        else:
            max_loss_day = self.config.max_loss_per_day
        logger.info(
            "Configurando sessão | loginid=%s | tipo=%s | saldo=%.2f | modo=%s | max_loss_dia=%.2f",
            loginid,
            account_type,
            balance,
            mode,
            max_loss_day,
        )

        # Frequency-based loss pause: disabled when LOSS_PAUSE_ENABLED=false or demo account
        _loss_pause_window = (
            9999 if (is_demo or not self.config.loss_pause_enabled) else 2
        )
        self.risk = RiskManager(
            balance=balance,
            max_loss_day=max_loss_day,
            max_profit_day=self.config.max_profit_per_day,
            max_trades_day=self.config.max_trades_per_day,
            daily_trailing_start=self.config.daily_trailing_start,
            daily_trailing_lock=self.config.daily_trailing_lock,
            max_stake_pct=self.config.max_stake_percent,
            fixed_stake=self.config.stake,
            min_stake=self.config.min_stake,
            max_stake=self.config.max_stake,
            max_consecutive_losses=self.config.max_consecutive_losses,
            use_soros=self.config.use_soros,
            soros_max_steps=self.config.soros_max_steps,
            soros_profit_factor=self.config.soros_profit_factor,
            dynamic_stake_base_pct=self.config.dynamic_stake_base_pct,
            use_martingale=self.config.use_martingale,
            martingale_max_gales=self.config.martingale_max_gales,
            martingale_multiplier=self.config.martingale_multiplier,
            martingale_payout_rate=self.config.martingale_payout_rate,
            martingale_max_balance_pct=self.config.martingale_max_balance_pct,
            martingale_min_balance_floor=self.config.martingale_min_balance_floor,
            martingale_lock_config=self.config.martingale_lock_config,
            martingale_mode=self.config.martingale_mode,
            max_losses_in_window=_loss_pause_window,
            stop_loss_pct=self.config.stop_loss_pct,
            stop_gain_pct=self.config.stop_gain_pct,
            simulated_balance=self.config.simulated_balance,
        )
        # Subscribe to real-time balance updates (catches manual top-ups, etc.)
        await self.subscribe_balance(ws)
        self._flush_balance(self.risk.balance)  # seed balance.json imediatamente após autorização
        # Reconcile P&L counters from DB (fixes drift after restarts)
        db_summary = self.journal.get_daily_summary(self.risk.day)

        if db_summary:
            self.risk.reconcile_pnl(db_summary)
        # Zombie-trade protection: reconcile open positions before subscribing ticks
        await self._reconcile_open_positions(ws)
        await self.subscribe_ticks(ws)

    async def handle_authorize(
        self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]
    ) -> None:
        auth = data["authorize"]
        balance = float(auth["balance"])
        loginid = str(auth.get("loginid", ""))
        is_demo = loginid.upper().startswith("VRTC")
        await self.initialize_risk_and_subscriptions(ws, balance, loginid, is_demo)

    async def handle_history(self, data: dict[str, Any]) -> None:
        history = data.get("history", {})
        times = history.get("times", [])
        prices = history.get("prices", [])
        for epoch, quote in zip(times, prices):
            self._append_tick({"epoch": epoch, "quote": quote})
        logger.info("%s ticks carregados.", len(self.tick_buffer))

    async def handle_tick(
        self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]
    ) -> None:
        tick = data["tick"]
        tick_time = float(tick.get("epoch", 0))
        receive_time = time.time()
        self._last_tick_time = receive_time  # watchdog heartbeat
        latency_ms = (receive_time - tick_time) * 1000 if tick_time > 0 else 0.0
        if latency_ms > MAX_TICK_LATENCY_SECONDS * 1000:
            logger.warning(
                "Tick atrasado ignorado: latencia=%.0fms > %.0fms epoch=%s",
                latency_ms,
                MAX_TICK_LATENCY_SECONDS * 1000,
                tick_time,
            )
            # Still buffer the tick for continuity but do not signal entry
            self._append_tick({"epoch": tick["epoch"], "quote": tick["quote"]})
            return

        is_new_tick = self._append_tick(
            {"epoch": tick["epoch"], "quote": tick["quote"]}
        )
        if is_new_tick:
            try:
                self._tick_queue.put_nowait(
                    {"epoch": int(tick["epoch"]), "quote": tick["quote"], "_ws": ws}
                )
            except asyncio.QueueFull:
                logger.warning(
                    "Fila de ticks cheia. Tick %s descartado da fila de analise.",
                    tick["epoch"],
                )

    async def handle_buy(
        self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]
    ) -> None:
        buy = data["buy"]
        contract_id = int(buy["contract_id"])
        buy_price_actual = float(
            buy.get("buy_price")
            or (self.pending_order.stake if self.pending_order else 0.0)
        )
        # Atualiza saldo imediatamente com o valor confirmado pela Deriv (evita atraso do stream balance)
        if self.risk is not None:
            if getattr(self.risk, "simulated_balance_mode", False):
                # Simulated balance: deduct the actual stake locally immediately
                self.risk.balance = round(self.risk.balance - buy_price_actual, 2)
                logger.info(
                    "💰 Saldo Simulado: %.2f (deduzido stake de %.2f)",
                    self.risk.balance,
                    buy_price_actual,
                )
                self.risk.sync_pnl_from_balance()
                self._flush_balance(self.risk.balance)
            else:
                bal_after = buy.get("balance_after")
                if bal_after is not None:
                    new_bal = float(bal_after)
                    if abs(new_bal - self.risk.balance) > 0.01:
                        logger.info(
                            "Saldo Deriv: %.2f → %.2f", self.risk.balance, new_bal
                        )
                        self.risk.balance = new_bal
                        self.risk.sync_pnl_from_balance()
                        self._flush_balance(new_bal)
            # Tell update() that the stake was already deducted, so it only needs
            # to add back stake+profit on WIN and do nothing on LOSS.
            self.risk._pending_stake_deduction = buy_price_actual

        if self._gale_expected > 0:
            # Sequential multi-contract gale: contracts are fired one at a time.
            # Next contract is only dispatched after current one settles as WIN.
            # Deriv only allows 1 open ACCU contract at a time (OpenPositionLimitExceeded).
            self._gale_ids.add(contract_id)
            self._gale_id_stakes[contract_id] = (
                buy_price_actual
                if buy_price_actual > 0
                else (self.pending_order.stake if self.pending_order else 0.0)
            )
            if self._gale_order is None:
                # First contract: record entry epoch and the canonical order for logging.
                self._gale_order = self.pending_order
                if self.pending_order:
                    self.last_accumulator_entry_epoch = self.pending_order.entry_epoch
                    self.accumulator_open_epoch = self.pending_order.entry_epoch
            logger.info(
                "GALE seq: contrato %d/%d aberto id=%s buy_price=%s — aguardando liquidacao",
                len(self._gale_ids),
                self._gale_expected,
                contract_id,
                buy.get("buy_price"),
            )
            await self.subscribe_contract(ws, contract_id)
            # Clear pending_order so evaluate_tick does not stall on proposal check.
            # waiting_for_result stays True — next contract is dispatched on WIN settlement.
            self.pending_order = None
        elif self.config.contract_mode in {"rise_fall", "jump_rise_fall"}:
            # Rise/Fall (or JumpRF): single binary contract per gale step, auto-settles.
            self.current_contract_id = contract_id
            if self.pending_order:
                direction = getattr(self.pending_order, "direction", "RF")
                self.last_rf_entry_epoch = self.pending_order.entry_epoch
                self.accumulator_open_epoch = self.pending_order.entry_epoch
            logger.info(
                "Contrato RF %s aberto: id=%s buy_price=%s",
                direction,
                contract_id,
                buy.get("buy_price"),
            )
            await self.subscribe_contract(ws, contract_id)
        else:
            # Normal single-contract ACCU mode (unchanged).
            self.current_contract_id = contract_id
            if self.pending_order:
                self.last_accumulator_entry_epoch = self.pending_order.entry_epoch
                self.accumulator_open_epoch = self.pending_order.entry_epoch
                self.accumulator_sell_requested = False
            logger.info(
                "Contrato ACCU aberto: id=%s buy_price=%s",
                contract_id,
                buy.get("buy_price"),
            )
            await self.subscribe_contract(ws, contract_id)

    async def handle_contract_update(
        self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]
    ) -> None:
        if not self.risk:
            logger.warning("Resultado recebido antes do RiskManager.")
            return

        contract = data["proposal_open_contract"]
        contract_id = int(contract.get("contract_id") or self.current_contract_id or 0)
        if not contract_id:
            logger.warning("Atualizacao de contrato sem contract_id.")
            return

        # "lost"/"cancelled" = barrier breach or knocked-out (accumulator loses its stake)
        is_sold = (
            contract.get("status") in {"sold", "lost", "cancelled"}
            or bool(contract.get("is_sold"))
            or bool(contract.get("is_expired"))
        )
        if not is_sold:
            # Rise/Fall contracts settle automatically — no monitoring needed.
            if self.config.contract_mode in {"rise_fall", "jump_rise_fall"}:
                return

            # Multi-gale: per-contract open-position monitoring.
            if self._gale_expected > 0 and contract_id in self._gale_ids:
                if contract_id not in self._gale_sell_requested:
                    # Barrier proximity check (same defensive logic as single-contract).
                    high_barrier = contract.get("high_barrier")
                    low_barrier = contract.get("low_barrier")
                    current_spot = float(contract.get("current_spot") or 0)
                    if high_barrier and low_barrier and current_spot > 0:
                        hb = float(high_barrier)
                        lb = float(low_barrier)
                        dist_low = (current_spot - lb) / current_spot * 100
                        dist_high = (hb - current_spot) / current_spot * 100
                        min_dist = min(dist_low, dist_high)
                        threshold = self.config.accumulator_min_barrier_distance_pct
                        if threshold > 0 and min_dist <= threshold:
                            logger.warning(
                                "⚠️ GALE multi BARREIRA PROXIMA id=%s dist=%.5f%% — saida defensiva",
                                contract_id,
                                min_dist,
                            )
                            sell_price = float(contract.get("bid_price", 0.0) or 0.0)
                            self._gale_sell_requested.add(contract_id)
                            await self.send(
                                ws,
                                {
                                    "sell": contract_id,
                                    "price": round(float(sell_price), 2),
                                },
                            )
                            return
                    # Per-contract TP / max-hold check.
                    stake_i = self._gale_id_stakes.get(contract_id, 0.0)
                    if stake_i > 0:
                        profit_i = float(contract.get("profit", 0.0))
                        target_i = (
                            stake_i * self.config.accumulator_take_profit_percent / 100
                        )
                        current_spot_time = int(
                            contract.get("current_spot_time")
                            or contract.get("date_start")
                            or 0
                        )
                        held_ticks = max(
                            0,
                            current_spot_time
                            - (self.accumulator_open_epoch or current_spot_time),
                        )
                        if (
                            profit_i >= target_i
                            or held_ticks >= self.config.accumulator_max_hold_ticks
                        ):
                            sell_price = float(contract.get("bid_price", 0.0) or 0.0)
                            reason = (
                                "take_profit"
                                if profit_i >= target_i
                                else "max_hold_ticks"
                            )
                            logger.info(
                                "GALE multi fechando id=%s por %s | profit=%.2f alvo=%.2f",
                                contract_id,
                                reason,
                                profit_i,
                                target_i,
                            )
                            self._gale_sell_requested.add(contract_id)
                            await self.send(
                                ws,
                                {
                                    "sell": contract_id,
                                    "price": round(float(sell_price), 2),
                                },
                            )
                return

            order = self.pending_order
            if order and not self.accumulator_sell_requested:
                # --- Barreira real: saida defensiva se spot muito proximo da barreira ---
                high_barrier = contract.get("high_barrier")
                low_barrier = contract.get("low_barrier")
                current_spot = float(contract.get("current_spot") or 0)
                if high_barrier and low_barrier and current_spot > 0:
                    hb = float(high_barrier)
                    lb = float(low_barrier)
                    dist_low = (current_spot - lb) / current_spot * 100
                    dist_high = (hb - current_spot) / current_spot * 100
                    min_dist = min(dist_low, dist_high)
                    logger.debug(
                        "Barreira: spot=%.5f low=%.5f high=%.5f dist_min=%.5f%%",
                        current_spot,
                        lb,
                        hb,
                        min_dist,
                    )
                    threshold = self.config.accumulator_min_barrier_distance_pct
                    if threshold > 0 and min_dist <= threshold:
                        logger.warning(
                            "⚠️ BARREIRA PROXIMA! dist=%.5f%% <= %.5f%% — saida defensiva",
                            min_dist,
                            threshold,
                        )
                        sell_price = float(contract.get("bid_price", 0.0) or 0.0)
                        await self.sell_contract(ws, contract_id, sell_price)
                        return
                # --- Lucro / tempo maximo ---
                profit = float(contract.get("profit", 0.0))
                target_profit = (
                    order.stake * self.config.accumulator_take_profit_percent / 100
                )
                current_spot_time = int(
                    contract.get("current_spot_time") or contract.get("date_start") or 0
                )
                held_ticks = max(
                    0,
                    current_spot_time
                    - (self.accumulator_open_epoch or current_spot_time),
                )
                if (
                    profit >= target_profit
                    or held_ticks >= self.config.accumulator_max_hold_ticks
                ):
                    sell_price = float(contract.get("bid_price", 0.0) or 0.0)
                    reason = (
                        "take_profit" if profit >= target_profit else "max_hold_ticks"
                    )
                    logger.info(
                        "Fechando ACCU por %s | profit=%.2f alvo=%.2f held_ticks=%s",
                        reason,
                        profit,
                        target_profit,
                        held_ticks,
                    )
                    await self.sell_contract(ws, contract_id, sell_price)
            return

        if contract_id in self.settled_contract_ids:
            return

        self.settled_contract_ids.add(contract_id)

        profit = float(contract.get("profit", 0.0))
        buy_price = float(contract.get("buy_price", 0.0))

        # Multi-gale settlement: collect each contract's result; aggregate when all done.
        if self._gale_expected > 0 and contract_id in self._gale_ids:
            self._gale_settled[contract_id] = profit
            is_win = profit >= 0
            logger.info(
                "GALE seq: contrato %d/%d liquidado id=%s profit=%.2f %s",
                len(self._gale_settled),
                self._gale_expected,
                contract_id,
                profit,
                "WIN" if is_win else "LOSS",
            )
            if is_win and self._gale_queue:
                # Sequential WIN — dispatch next contract now.
                next_stake = self._gale_queue.pop(0)
                po = self._gale_order
                self.pending_order = PendingOrder(
                    stake=next_stake,
                    score=po.score if po else 0,
                    entry_epoch=po.entry_epoch if po else 0,
                    metrics=po.metrics if po else None,
                )
                logger.info(
                    "GALE seq: WIN %d/%d — disparando proximo stake=%.2f (%d restantes na fila)",
                    len(self._gale_settled),
                    self._gale_expected,
                    next_stake,
                    len(self._gale_queue),
                )
                await self._request_gale_proposal(ws, next_stake)
                return  # waiting_for_result stays True

            if not is_win and self._gale_queue:
                logger.warning(
                    "GALE seq: LOSS em %d/%d — abortando sequencia (%d contratos nao executados)",
                    len(self._gale_settled),
                    self._gale_expected,
                    len(self._gale_queue),
                )
                self._gale_queue = []  # discard remaining — loss already triggered

            # All done: either all WIN or first LOSS — aggregate and process as single trade.
            total_profit = sum(self._gale_settled.values())
            total_buy_price = sum(self._gale_id_stakes.values())
            order = self._gale_order
            exit_epoch = (
                int(
                    contract.get("sell_time")
                    or contract.get("current_spot_time")
                    or contract.get("date_expiry")
                    or 0
                )
                or None
            )
            held_ticks = (
                max(0, exit_epoch - order.entry_epoch)
                if (exit_epoch and order)
                else None
            )
            _pre_soros_step = self.risk.soros_step
            _pre_gale_step = self.risk.martingale_step
            logger.info(
                "GALE seq: todos %d/%d contratos liquidados | total_profit=%.2f total_buy=%.2f",
                len(self._gale_settled),
                self._gale_expected,
                total_profit,
                total_buy_price,
            )
            if order:
                self.journal.log_trade(
                    symbol=self.config.symbol,
                    contract_mode=self.config.contract_mode,
                    contract_id=contract_id,
                    entry_epoch=order.entry_epoch,
                    direction=order.direction,
                    score=order.score,
                    stake=self._gale_total_stake,
                    buy_price=total_buy_price,
                    profit=total_profit,
                    exit_epoch=exit_epoch,
                    held_ticks=held_ticks,
                    metrics=order.metrics,
                    soros_step=_pre_soros_step,
                    gale_step=_pre_gale_step,
                )
            _prev_m_step = _pre_gale_step
            self.risk.update(profit=total_profit, buy_price=total_buy_price)
            self._flush_balance(self.risk.balance)  # atualiza dashboard imediatamente
            if getattr(self.risk, "use_martingale", False):
                if total_profit < 0 and self.risk.martingale_step > _prev_m_step:
                    logger.warning(
                        "\u26a0 GALE %d/%d ativado (multi) | pr\u00f3xima stake ser\u00e1 maior",
                        self.risk.martingale_step,
                        self.risk.martingale_max_gales,
                    )
                elif total_profit > 0 and _prev_m_step > 0:
                    logger.info(
                        "\u2705 GALE %d (multi) recuperado \u2014 stake volta ao normal",
                        _prev_m_step,
                    )
            logger.info(self.risk.stats())
            self._reset_gale_state()
            self.waiting_for_result = False
            self.current_contract_id = None
            self.pending_order = None
            self.accumulator_open_epoch = None
            self.accumulator_sell_requested = False
            # +15 offset: effective cooldown = cooldown_ticks + 16 ticks (~21s) after settlement
            # Deriv holds the ACCU buy-lock for ~20s after any settlement.
            self.last_accumulator_entry_epoch = int(time.time()) + 15
            return

        order = self.pending_order
        if not order:
            # Zombie trade settled — no pending order but contract was real.
            # Count profit/loss in risk manager to keep stats accurate.
            logger.warning(
                "Zombie trade liquidado (contract_id=%s profit=%.2f buy_price=%.2f) — contabilizando.",
                contract_id,
                profit,
                buy_price,
            )
            if self.risk:
                self.risk.update(profit=profit, buy_price=buy_price)
                self._flush_balance(
                    self.risk.balance
                )  # atualiza dashboard imediatamente
                logger.info(self.risk.stats())
            self.waiting_for_result = False
            self.current_contract_id = None
            self.accumulator_open_epoch = None
            self.accumulator_sell_requested = False
            # +15 offset: effective cooldown = cooldown_ticks + 16 ticks (~21s) after settlement
            self.last_accumulator_entry_epoch = int(time.time()) + 15
            return
        exit_epoch = (
            int(
                contract.get("sell_time")
                or contract.get("current_spot_time")
                or contract.get("date_expiry")
                or 0
            )
            or None
        )
        held_ticks = (
            max(0, exit_epoch - order.entry_epoch) if exit_epoch is not None else None
        )
        _pre_soros_step = self.risk.soros_step
        _pre_gale_step = self.risk.martingale_step
        self.journal.log_trade(
            symbol=self.config.symbol,
            contract_mode=self.config.contract_mode,
            contract_id=contract_id,
            entry_epoch=order.entry_epoch,
            direction=order.direction,
            score=order.score,
            stake=order.stake,
            buy_price=buy_price,
            profit=profit,
            exit_epoch=exit_epoch,
            held_ticks=held_ticks,
            metrics=order.metrics,
            soros_step=_pre_soros_step,
            gale_step=_pre_gale_step,
        )
        _prev_m_step = _pre_gale_step
        # Lock gale direction at LOSS time — inherit from the trade that just lost
        if (
            profit < 0
            and getattr(self.risk, "use_martingale", False)
            and order.direction in {"CALL", "PUT"}
        ):
            self._gale_locked_direction = order.direction
        self.risk.update(profit=profit, buy_price=buy_price)
        self._flush_balance(self.risk.balance)  # atualiza dashboard imediatamente
        # Log gale state transitions
        if getattr(self.risk, "use_martingale", False):
            if profit < 0 and self.risk.martingale_step > _prev_m_step:
                _raw = self.risk.get_gale_raw_stake()
                _acum = self.risk.martingale_accumulated_loss
                logger.warning(
                    "\u26a0 GALE %d/%d ativado | acum_loss=%.2f | pr\u00f3xima stake bruta=%.2f | dir_lock=%s",
                    self.risk.martingale_step,
                    self.risk.martingale_max_gales,
                    _acum,
                    _raw,
                    self._gale_locked_direction,
                )
            elif profit > 0 and _prev_m_step > 0:
                logger.info(
                    "\u2705 GALE %d recuperado \u2014 stake volta ao normal",
                    _prev_m_step,
                )
                self._gale_locked_direction = None
            elif profit < 0 and self.risk.martingale_step == 0 and _prev_m_step > 0:
                # Gale sequence exhausted — set cooldown
                self._gale_exhaustion_epoch = order.entry_epoch
                self._gale_locked_direction = None
                logger.warning("GALE EXHAUSTED — cooldown ativado por 30 ticks")
        logger.info(self.risk.stats())

        self.waiting_for_result = False
        self.current_contract_id = None
        self.pending_order = None
        self.accumulator_open_epoch = None
        self.accumulator_sell_requested = False
        # +15 offset: effective cooldown = cooldown_ticks + 16 ticks (~21s) after settlement
        # Deriv holds the ACCU buy-lock for ~20s after WIN/LOSS — must wait before next BUY.
        self.last_accumulator_entry_epoch = int(time.time()) + 15

    async def handle_message(
        self, ws: websockets.WebSocketClientProtocol, message: str
    ) -> None:
        data = json.loads(message)

        if "error" in data:
            error = data["error"]
            logger.error(
                "Erro da API (%s): %s", error.get("code"), error.get("message")
            )
            if data.get("msg_type") == "authorize":
                err_code = error.get("code", "")
                err_msg = error.get("message", "")
                # Erros de CONFIGURACAO (token errado, conta errada) -> fatal, nao tem como recuperar
                _fatal_codes = {
                    "InvalidToken",
                    "InvalidAppID",
                    "AuthorizationRequired",
                    "DisabledClient",
                    "AccountUnavailable",
                    "PermissionDenied",
                }
                if err_code in _fatal_codes:
                    raise FatalBotError(f"Falha na autorizacao [{err_code}]: {err_msg}")
                # Erros TRANSIENTES da Deriv (WrongResponse, server error, etc) -> loga e reconecta
                logger.warning(
                    "Erro transiente na autorizacao [%s]: %s — reconectando em %ss",
                    err_code,
                    err_msg,
                    self.config.reconnect_delay_seconds,
                )
                raise ConnectionError(f"Auth transiente [{err_code}]: {err_msg}")
            if data.get("msg_type") in {"proposal", "buy"}:
                err_code = error.get("code", "")
                if (
                    data.get("msg_type") == "buy"
                    and err_code == "OpenPositionLimitExceeded"
                ):
                    # Deriv rejeitou o buy pois já existe um contrato aberto.
                    # Manter waiting_for_result=True para bloquear novas tentativas,
                    # e reconciliar portfolio para encontrar e subscrever o contrato existente.
                    logger.warning(
                        "OpenPositionLimitExceeded: contrato ja aberto no Deriv — reconciliando portfolio..."
                    )
                    self.waiting_for_result = True
                    self.pending_order = None
                    # Force extended cooldown: +15 offset so bot waits ~21 ticks after error.
                    # Deriv keeps the ACCU buy-lock for ~20-25s after settlement;
                    # this ensures we don't retry until the slot is actually free.
                    self.last_accumulator_entry_epoch = int(time.time()) + 15
                    await self._reconcile_open_positions(ws)
                else:
                    self._reset_gale_state()
                    self.pending_order = None
                    self.waiting_for_result = False
            if data.get("msg_type") == "sell":
                self.accumulator_sell_requested = False
            return

        msg_type = data.get("msg_type")
        if msg_type == "authorize":
            await self.handle_authorize(ws, data)
        elif msg_type == "history":
            await self.handle_history(data)
        elif msg_type == "tick":
            await self.handle_tick(ws, data)
        elif msg_type == "proposal":
            await self.buy_from_proposal(ws, data["proposal"])
        elif msg_type == "buy":
            await self.handle_buy(ws, data)
        elif msg_type == "proposal_open_contract":
            await self.handle_contract_update(ws, data)
        elif msg_type == "balance":
            bal = data.get("balance", {})
            new_bal = bal.get("balance")
            if new_bal is not None and self.risk is not None:
                new_bal = float(new_bal)
                if getattr(self.risk, "simulated_balance_mode", False):
                    logger.debug("Banca simulada ativa. Saldo real %s ignorado.", new_bal)
                else:
                    if abs(new_bal - self.risk.balance) > 0.01:
                        logger.info(
                            "Saldo Deriv: %.2f → %.2f", self.risk.balance, new_bal
                        )
                        self.risk.balance = new_bal
                        self.risk.sync_pnl_from_balance()
                        self._flush_balance(new_bal)
        elif msg_type == "sell":
            sell = data.get("sell", {})
            logger.info("Sell confirmado: %s", sell)
            if self.risk is not None:
                bal_after = sell.get("balance_after")
                if bal_after is not None:
                    new_bal = float(bal_after)
                    if getattr(self.risk, "simulated_balance_mode", False):
                        logger.debug("Banca simulada ativa. Saldo real pós-venda %s ignorado.", new_bal)
                    else:
                        if abs(new_bal - self.risk.balance) > 0.01:
                            logger.info(
                                "Saldo Deriv: %.2f → %.2f", self.risk.balance, new_bal
                            )
                            self.risk.balance = new_bal
                            self.risk.sync_pnl_from_balance()
                            self._flush_balance(new_bal)
        elif msg_type == "portfolio":
            await self.handle_portfolio(ws, data)
        elif msg_type == "ping":
            logger.debug("Ping recebido.")
        else:
            logger.debug("Mensagem ignorada: %s", msg_type)

    async def _tick_consumer(self) -> None:
        """Consumer coroutine: drains _tick_queue and calls evaluate_tick."""
        while True:
            item = await self._tick_queue.get()
            ws = item["_ws"]
            epoch = item["epoch"]
            try:
                await self.evaluate_tick(ws, epoch)
            except Exception as exc:  # pragma: no cover - surface unexpected errors
                logger.error("Erro no consumer de ticks: %s", exc)
            finally:
                self._tick_queue.task_done()

    async def _reconcile_open_positions(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        """Zombie-trade protection: check for open contracts on reconnect."""
        logger.info(
            "Verificando contratos abertos no portfolio (protecao zombie trade)..."
        )
        if self.config.contract_mode in {"rise_fall", "jump_rise_fall"}:
            await self.send(ws, {"portfolio": 1, "contract_type": ["CALL", "PUT"]})
        else:
            await self.send(ws, {"portfolio": 1, "contract_type": ["ACCU"]})

    async def handle_portfolio(
        self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]
    ) -> None:
        portfolio = data.get("portfolio", {})
        contracts = portfolio.get("contracts", [])
        open_contracts = [
            c for c in contracts if not c.get("is_sold") and not c.get("is_expired")
        ]
        mode_label = (
            "RF"
            if self.config.contract_mode in {"rise_fall", "jump_rise_fall"}
            else "ACCU"
        )
        if not open_contracts:
            logger.info("Portfolio: nenhum contrato %s aberto encontrado.", mode_label)
            if self.waiting_for_result or self.pending_order:
                logger.warning(
                    "Portfolio vazio mas waiting_for_result=%s pending_order=%s — resetando estado.",
                    self.waiting_for_result,
                    self.pending_order is not None,
                )
                self._reset_gale_state()
                self.waiting_for_result = False
                self.pending_order = None
                self.current_contract_id = None
                self.accumulator_sell_requested = False
            elif self._stale_pending_order is not None:
                # A trade was open during disconnect and has since settled on Deriv's
                # side. We don't know the outcome — treat as LOSS so the risk manager
                # doesn't under-count daily losses.
                stale = self._stale_pending_order
                self._stale_pending_order = None
                if self.risk:
                    logger.warning(
                        "Trade %s aberto durante desconexao ja expirou — contabilizando como LOSS stake=%.2f.",
                        stale.direction,
                        stale.stake,
                    )
                    self.risk.update(profit=-stale.stake, buy_price=stale.stake)
                    self._flush_balance(
                        self.risk.balance
                    )  # atualiza dashboard imediatamente
                    logger.info(self.risk.stats())
            return
        for contract in open_contracts:
            cid = int(contract.get("contract_id", 0))
            if not cid:
                continue
            if (
                self.current_contract_id is None
                and cid not in self.settled_contract_ids
            ):
                logger.warning(
                    "Zombie trade detectado: contrato %s id=%s aberto sem rastreamento local. Subscrevendo.",
                    mode_label,
                    cid,
                )
                self.current_contract_id = cid
                self.waiting_for_result = True
                await self.subscribe_contract(ws, cid)

    async def _tick_watchdog(
        self, ws: websockets.WebSocketClientProtocol, timeout: float = 60.0
    ) -> None:
        """Detecta ausência de ticks por mais de `timeout` segundos e força reconexão."""
        await asyncio.sleep(timeout)  # grace period inicial
        while True:
            await asyncio.sleep(30.0)
            elapsed = time.time() - self._last_tick_time
            if elapsed > timeout:
                logger.error(
                    "Watchdog: nenhum tick recebido nos últimos %.0fs — forçando reconexão.",
                    elapsed,
                )
                await ws.close()
                return

    async def run_forever(self) -> None:
        while True:
            # Preserve pending state across reconnects so _reconcile_open_positions
            # can account for a trade that settled while the WebSocket was down.
            self._stale_pending_order = (
                self.pending_order if self.waiting_for_result else None
            )
            # Reset transient per-connection state so every reconnect starts clean.
            # _reconcile_open_positions will re-establish any genuinely open contract.
            self._reset_gale_state()
            self.pending_order = None
            self.waiting_for_result = False
            self.current_contract_id = None
            self.accumulator_sell_requested = False
            try:
                _mode_desc = {
                    "accumulator": "Accumulators 1s",
                    "calm_accu": "Calm ACCU (BOOM1000)",
                    "rise_fall": "Rise/Fall",
                    "jump_rise_fall": "JumpRF Momentum",
                }.get(self.config.contract_mode, self.config.contract_mode)
                # Obter URL WebSocket e credenciais dinamicamente via deriv_auth (novo sistema ou legado)
                auth = deriv_auth.get_auth(self.config.app_id, self.config.account_mode)

                logger.info(
                    "Iniciando %s | %s | ativo=%s | endpoint=%s",
                    self.config.bot_name,
                    _mode_desc,
                    self.config.symbol,
                    auth.ws_url,
                )
                async with websockets.connect(
                    auth.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    self._last_tick_time = (
                        time.time()
                    )  # reset watchdog on every new connection
                    self._gale_wait_log_ts = 0.0  # reset throttle log
                    self._gale_wait_ticks = 0
                    self.last_rf_entry_epoch = None  # reset RF cooldown on reconnect
                    
                    if not auth.is_new_api:
                        await self.authorize(ws, auth.legacy_token)
                    else:
                        logger.info("Nova API: Conexão WebSocket estabelecida e pré-autenticada via OTP.")
                        is_demo = auth.account_id.upper().startswith("VRTC") or auth.account_type == "demo"
                        await self.initialize_risk_and_subscriptions(
                            ws,
                            balance=auth.balance,
                            loginid=auth.account_id,
                            is_demo=is_demo,
                        )
                    # Start EDA consumer task and watchdog
                    consumer_task = asyncio.create_task(self._tick_consumer())
                    watchdog_task = asyncio.create_task(
                        self._tick_watchdog(ws, timeout=60.0)
                    )
                    try:
                        async for message in ws:
                            await self.handle_message(ws, message)
                    finally:
                        consumer_task.cancel()
                        watchdog_task.cancel()
                        try:
                            await consumer_task
                        except asyncio.CancelledError:
                            pass
                        try:
                            await watchdog_task
                        except asyncio.CancelledError:
                            pass
            except asyncio.CancelledError:
                raise
            except FatalBotError as exc:
                logger.error("Erro fatal: %s", exc)
                return
            except (
                ConnectionClosed,
                OSError,
                RuntimeError,
                json.JSONDecodeError,
            ) as exc:
                logger.error("Conexao/execucao interrompida: %s", exc)
                logger.info(
                    "Reconectando em %s segundos...",
                    self.config.reconnect_delay_seconds,
                )
                await asyncio.sleep(self.config.reconnect_delay_seconds)


async def main() -> None:
    config = load_config()
    bot = DerivBot(config)
    await bot.run_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot encerrado pelo usuario.")
