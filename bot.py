from __future__ import annotations

import asyncio
import json
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
from strategy import calculate_tick_indicators, generate_accumulator_signal


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

    async def send(self, ws: websockets.WebSocketClientProtocol, payload: dict[str, Any]) -> None:
        await ws.send(json.dumps(payload))

    async def authorize(self, ws: websockets.WebSocketClientProtocol) -> None:
        await self.send(ws, {"authorize": self.config.token})

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
            logger.info("Aguardando resultado da operacao ACCU anterior.")
            return

        if self.pending_order:
            logger.info("Aguardando proposta/compra ACCU pendente.")
            return

        if not self.risk:
            logger.warning("RiskManager ainda nao inicializado.")
            return

        tick_hour = datetime.fromtimestamp(tick_epoch, UTC).hour
        if tick_hour in self.config.blocked_utc_hours:
            logger.info("Hora UTC bloqueada para novas entradas: %s", tick_hour)
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
        signal, score = generate_accumulator_signal(df, config=self.config.accumulator_strategy_config)

        if signal != "ACCU":
            logger.info("Sem setup ACCU no tick %s.", tick_epoch)
            return

        stake = self.risk.get_stake()
        metrics = self._last_accumulator_metrics(df)
        logger.info("Setup ACCU detectado: score=%s stake=%.2f", score, stake)
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
        logger.info("Autorizado | loginid=%s | tipo=%s | saldo=%.2f | modo=%s", loginid, account_type, balance, mode)

        self.risk = RiskManager(
            balance=balance,
            max_loss_day=self.config.max_loss_per_day,
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
        )
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
        is_new_tick = self._append_tick({"epoch": tick["epoch"], "quote": tick["quote"]})
        if is_new_tick:
            await self.evaluate_tick(ws, int(tick["epoch"]))

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

        is_sold = contract.get("status") == "sold" or bool(contract.get("is_sold")) or bool(contract.get("is_expired"))
        if not is_sold:
            order = self.pending_order
            if order and not self.accumulator_sell_requested:
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
        elif msg_type == "sell":
            logger.info("Sell confirmado: %s", data.get("sell"))
        elif msg_type == "ping":
            logger.debug("Ping recebido.")
        else:
            logger.debug("Mensagem ignorada: %s", msg_type)

    async def run_forever(self) -> None:
        while True:
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
                    async for message in ws:
                        await self.handle_message(ws, message)
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
