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
from journal import TradeJournal
from logger import logger
from risk_manager import RiskManager
from strategy import calculate_tick_indicators, generate_accumulator_signal, EnsembleScorer

#: Maximum acceptable tick age in seconds before an entry is skipped.
#: Configurable via MAX_TICK_LATENCY_MS env var (default 500ms to handle
#: real-world network latency between Deriv servers and the bot host).
MAX_TICK_LATENCY_SECONDS: float = float(os.getenv("MAX_TICK_LATENCY_MS", "500")) / 1000.0


class FatalBotError(RuntimeError):
    pass


@dataclass
class PendingOrder:
    stake: float
    score: int
    entry_epoch: int
    metrics: dict[str, Any] | None = None


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
        self.journal = TradeJournal(config.journal_dir)
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

    async def send(self, ws: websockets.WebSocketClientProtocol, payload: dict[str, Any]) -> None:
        await ws.send(json.dumps(payload))

    async def authorize(self, ws: websockets.WebSocketClientProtocol) -> None:
        await self.send(ws, {"authorize": self.config.token})

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

        logger.info("Solicitando proposta ACCU | stake=%.2f | ativo=%s | epoch=%s", stake, self.config.symbol, entry_epoch)
        payload: dict[str, Any] = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": "ACCU",
            "currency": self.config.currency,
            "symbol": self.config.symbol,
            "growth_rate": self.config.accumulator_growth_rate,
        }

        if self.config.accumulator_use_limit_order:
            payload["limit_order"] = {
                "take_profit": round(stake * self.config.accumulator_take_profit_percent / 100, 2)
            }

        await self.send(ws, payload)

    async def buy_from_proposal(self, ws: websockets.WebSocketClientProtocol, proposal: dict[str, Any]) -> None:
        if not self.pending_order:
            logger.warning("Proposta recebida sem ordem pendente.")
            return

        proposal_id = proposal.get("id")
        ask_price = proposal.get("ask_price", self.pending_order.stake)
        if not proposal_id:
            logger.error("Proposta sem id: %s", proposal)
            self.pending_order = None
            return

        self.waiting_for_result = True
        self._waiting_since = time.monotonic()
        logger.info("Comprando ACCU | proposal_id=%s | price=%s", proposal_id, ask_price)
        await self.send(ws, {"buy": proposal_id, "price": ask_price})

    async def subscribe_contract(self, ws: websockets.WebSocketClientProtocol, contract_id: int) -> None:
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

    async def evaluate_tick(self, ws: websockets.WebSocketClientProtocol, tick_epoch: int) -> None:
        if self.waiting_for_result:
            stuck_sec = time.monotonic() - self._waiting_since
            if stuck_sec > 120:
                logger.warning(
                    "waiting_for_result timeout (%.0fs sem resposta) — resetando estado e retomando operacoes.",
                    stuck_sec,
                )
                self.waiting_for_result = False
                self.pending_order = None
                self.current_contract_id = None
                self.accumulator_sell_requested = False
            else:
                logger.info("Aguardando resultado da operacao ACCU anterior.")
                return

        if self.pending_order:
            logger.info("Aguardando proposta/compra ACCU pendente.")
            return

        if not self.risk:
            logger.warning("RiskManager ainda nao inicializado.")
            return

        tick_dt = datetime.fromtimestamp(tick_epoch, UTC)
        tick_hour = tick_dt.hour
        if tick_hour in self.config.blocked_utc_hours:
            logger.info("Hora UTC bloqueada para novas entradas: %s", tick_hour)
            return

        # Block weekends: Friday 21:00 UTC → Sunday 21:00 UTC
        if self.config.block_weekends:
            dow = tick_dt.weekday()  # 0=Mon … 4=Fri, 5=Sat, 6=Sun
            is_blocked_period = (
                dow == 6  # all Sunday
                or dow == 5  # all Saturday
                or (dow == 4 and tick_hour >= 21)  # Friday from 21:00 UTC
            )
            if is_blocked_period:
                logger.info("Fim de semana bloqueado (BLOCK_WEEKENDS=true): dow=%s hour=%s", dow, tick_hour)
                return

        if self.last_accumulator_entry_epoch is not None:
            ticks_since_entry = tick_epoch - self.last_accumulator_entry_epoch
            if ticks_since_entry <= self.config.accumulator_cooldown_ticks:
                logger.info(
                    "Cooldown ACCU ativo: %s tick(s) desde a ultima entrada; minimo=%s.",
                    ticks_since_entry,
                    self.config.accumulator_cooldown_ticks + 1,
                )
                return

        if not self.risk.can_trade():
            logger.warning("Bot pausado por regra de risco.")
            return

        df = calculate_tick_indicators(list(self.tick_buffer), config=self.config.accumulator_strategy_config)
        signal, score, p_loss = generate_accumulator_signal(
            df,
            config=self.config.accumulator_strategy_config,
            ensemble_scorer=self._ensemble_scorer,
        )

        if signal != "ACCU":
            logger.info("Sem setup ACCU no tick %s.", tick_epoch)
            return

        stake = self.risk.get_stake(p_loss=p_loss)
        metrics = self._last_accumulator_metrics(df)
        logger.info(
            "Setup ACCU detectado: score=%s stake=%.2f p_loss=%s",
            score,
            stake,
            f"{p_loss:.4f}" if p_loss is not None else "N/A",
        )
        await self.request_accumulator_proposal(ws, stake, score, tick_epoch, metrics=metrics)

    @staticmethod
    def _last_accumulator_metrics(df: Any) -> dict[str, float]:
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
        ):
            try:
                value = float(last.get(name))
            except (TypeError, ValueError):
                continue
            if value == value:
                metrics[name] = value
        return metrics

    async def handle_authorize(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
        auth = data["authorize"]
        balance = float(auth["balance"])
        loginid = str(auth.get("loginid", ""))
        is_demo = loginid.upper().startswith("VRTC")

        if self.config.account_mode == "demo" and not is_demo:
            raise FatalBotError(
                f"ACCOUNT_MODE=demo, mas a API autorizou loginid={loginid}. Use token demo VRTC ou mude a config."
            )
        if self.config.account_mode == "real" and is_demo:
            raise FatalBotError(
                f"ACCOUNT_MODE=real, mas a API autorizou loginid={loginid}. Use token real ou mude a config."
            )
        if not self.config.dry_run and not is_demo and not self.config.allow_real_trading:
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
            "Autorizado | loginid=%s | tipo=%s | saldo=%.2f | modo=%s | max_loss_dia=%.2f",
            loginid, account_type, balance, mode, max_loss_day,
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
            use_dynamic_stake=self.config.use_dynamic_stake,
            dynamic_stake_base_pct=self.config.dynamic_stake_base_pct,
        )
        # Subscribe to real-time balance updates (catches manual top-ups, etc.)
        await self.subscribe_balance(ws)
        # Zombie-trade protection: reconcile open positions before subscribing ticks
        await self._reconcile_open_positions(ws)
        await self.subscribe_ticks(ws)

    async def handle_history(self, data: dict[str, Any]) -> None:
        history = data.get("history", {})
        times = history.get("times", [])
        prices = history.get("prices", [])
        for epoch, quote in zip(times, prices):
            self._append_tick({"epoch": epoch, "quote": quote})
        logger.info("%s ticks carregados.", len(self.tick_buffer))

    async def handle_tick(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
        tick = data["tick"]
        tick_time = float(tick.get("epoch", 0))
        receive_time = time.time()
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

        is_new_tick = self._append_tick({"epoch": tick["epoch"], "quote": tick["quote"]})
        if is_new_tick:
            try:
                self._tick_queue.put_nowait({"epoch": int(tick["epoch"]), "quote": tick["quote"], "_ws": ws})
            except asyncio.QueueFull:
                logger.warning("Fila de ticks cheia. Tick %s descartado da fila de analise.", tick["epoch"])

    async def handle_buy(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
        buy = data["buy"]
        contract_id = int(buy["contract_id"])
        self.current_contract_id = contract_id
        if self.pending_order:
            self.last_accumulator_entry_epoch = self.pending_order.entry_epoch
            self.accumulator_open_epoch = self.pending_order.entry_epoch
            self.accumulator_sell_requested = False
        logger.info("Contrato ACCU aberto: id=%s buy_price=%s", contract_id, buy.get("buy_price"))
        await self.subscribe_contract(ws, contract_id)

    async def handle_contract_update(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
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
                        current_spot, lb, hb, min_dist,
                    )
                    threshold = self.config.accumulator_min_barrier_distance_pct
                    if threshold > 0 and min_dist <= threshold:
                        logger.warning(
                            "⚠️ BARREIRA PROXIMA! dist=%.5f%% <= %.5f%% — saida defensiva",
                            min_dist, threshold,
                        )
                        sell_price = float(contract.get("bid_price", 0.0) or 0.0)
                        await self.sell_contract(ws, contract_id, sell_price)
                        return
                # --- Lucro / tempo maximo ---
                profit = float(contract.get("profit", 0.0))
                target_profit = order.stake * self.config.accumulator_take_profit_percent / 100
                current_spot_time = int(contract.get("current_spot_time") or contract.get("date_start") or 0)
                held_ticks = max(0, current_spot_time - (self.accumulator_open_epoch or current_spot_time))
                if profit >= target_profit or held_ticks >= self.config.accumulator_max_hold_ticks:
                    sell_price = float(contract.get("bid_price", 0.0) or 0.0)
                    reason = "take_profit" if profit >= target_profit else "max_hold_ticks"
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
        order = self.pending_order
        profit = float(contract.get("profit", 0.0))
        buy_price = float(contract.get("buy_price", 0.0))
        if order:
            exit_epoch = int(contract.get("sell_time") or contract.get("current_spot_time") or contract.get("date_expiry") or 0) or None
            held_ticks = max(0, exit_epoch - order.entry_epoch) if exit_epoch is not None else None
            self.journal.log_trade(
                symbol=self.config.symbol,
                contract_mode=self.config.contract_mode,
                contract_id=contract_id,
                entry_epoch=order.entry_epoch,
                direction="ACCU",
                score=order.score,
                stake=order.stake,
                buy_price=buy_price,
                profit=profit,
                exit_epoch=exit_epoch,
                held_ticks=held_ticks,
                metrics=order.metrics,
            )
        self.risk.update(profit=profit, buy_price=buy_price)
        logger.info(self.risk.stats())

        self.waiting_for_result = False
        self.current_contract_id = None
        self.pending_order = None
        self.accumulator_open_epoch = None
        self.accumulator_sell_requested = False

    async def handle_message(self, ws: websockets.WebSocketClientProtocol, message: str) -> None:
        data = json.loads(message)

        if "error" in data:
            error = data["error"]
            logger.error("Erro da API (%s): %s", error.get("code"), error.get("message"))
            if data.get("msg_type") == "authorize":
                raise FatalBotError(f"Falha na autorizacao: {error.get('message')}")
            if data.get("msg_type") in {"proposal", "buy"}:
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
                if abs(new_bal - self.risk.balance) > 0.01:
                    logger.info("Saldo Deriv: %.2f \u2192 %.2f", self.risk.balance, new_bal)
                    self.risk.balance = new_bal
        elif msg_type == "sell":
            logger.info("Sell confirmado: %s", data.get("sell"))
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

    async def _reconcile_open_positions(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Zombie-trade protection: check for open contracts on reconnect.

        Sends a portfolio request and, if an open ACCU contract is found while
        the bot thinks it has no position, marks it as the current contract so
        the bot can manage it properly.
        """
        logger.info("Verificando contratos abertos no portfolio (protecao zombie trade)...")
        await self.send(ws, {"portfolio": 1, "contract_type": ["ACCU"]})

    async def handle_portfolio(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
        portfolio = data.get("portfolio", {})
        contracts = portfolio.get("contracts", [])
        open_contracts = [c for c in contracts if not c.get("is_sold") and not c.get("is_expired")]
        if not open_contracts:
            logger.info("Portfolio: nenhum contrato ACCU aberto encontrado.")
            if self.waiting_for_result or self.pending_order:
                logger.warning(
                    "Portfolio vazio mas waiting_for_result=%s pending_order=%s — resetando estado.",
                    self.waiting_for_result,
                    self.pending_order is not None,
                )
                self.waiting_for_result = False
                self.pending_order = None
                self.current_contract_id = None
                self.accumulator_sell_requested = False
            return
        for contract in open_contracts:
            cid = int(contract.get("contract_id", 0))
            if not cid:
                continue
            if self.current_contract_id is None and cid not in self.settled_contract_ids:
                logger.warning(
                    "Zombie trade detectado: contrato ACCU id=%s aberto sem rastreamento local. Subscrevendo.",
                    cid,
                )
                self.current_contract_id = cid
                self.waiting_for_result = True
                await self.subscribe_contract(ws, cid)

    async def run_forever(self) -> None:
        while True:
            # Reset transient per-connection state so every reconnect starts clean.
            # _reconcile_open_positions will re-establish any genuinely open contract.
            self.pending_order = None
            self.waiting_for_result = False
            self.current_contract_id = None
            self.accumulator_sell_requested = False
            try:
                logger.info(
                    "Iniciando %s | Accumulators 1s | ativo=%s | endpoint=%s",
                    self.config.bot_name,
                    self.config.symbol,
                    self.config.ws_url,
                )
                async with websockets.connect(
                    self.config.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    await self.authorize(ws)
                    # Start EDA consumer task
                    consumer_task = asyncio.create_task(self._tick_consumer())
                    try:
                        async for message in ws:
                            await self.handle_message(ws, message)
                    finally:
                        consumer_task.cancel()
                        try:
                            await consumer_task
                        except asyncio.CancelledError:
                            pass
            except asyncio.CancelledError:
                raise
            except FatalBotError as exc:
                logger.error("Erro fatal: %s", exc)
                return
            except (ConnectionClosed, OSError, RuntimeError, json.JSONDecodeError) as exc:
                logger.error("Conexao/execucao interrompida: %s", exc)
                logger.info("Reconectando em %s segundos...", self.config.reconnect_delay_seconds)
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
