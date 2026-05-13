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
from strategy import calculate_indicators, generate_signal


class FatalBotError(RuntimeError):
    pass


@dataclass
class PendingOrder:
    direction: str
    stake: float
    score: int
    candle_epoch: int


class DerivBot:
    def __init__(self, config: BotConfig):
        self.config = config
        self.candle_buffer: deque[dict[str, Any]] = deque(maxlen=config.candle_count + 5)
        self.risk: Optional[RiskManager] = None
        self.pending_order: Optional[PendingOrder] = None
        self.waiting_for_result = False
        self.current_contract_id: Optional[int] = None
        self.settled_contract_ids: set[int] = set()
        self.last_evaluated_epoch: Optional[int] = None
        self.last_trade_candle_epoch: Optional[int] = None
        self.journal = TradeJournal(config.journal_dir)

    async def send(self, ws: websockets.WebSocketClientProtocol, payload: dict[str, Any]) -> None:
        await ws.send(json.dumps(payload))

    async def authorize(self, ws: websockets.WebSocketClientProtocol) -> None:
        await self.send(ws, {"authorize": self.config.token})

    async def subscribe_candles(self, ws: websockets.WebSocketClientProtocol) -> None:
        await self.send(
            ws,
            {
                "ticks_history": self.config.symbol,
                "count": self.config.candle_count,
                "end": "latest",
                "granularity": self.config.granularity,
                "style": "candles",
                "subscribe": 1,
            },
        )

    async def request_proposal(
        self,
        ws: websockets.WebSocketClientProtocol,
        direction: str,
        stake: float,
        score: int,
        candle_epoch: int,
    ) -> None:
        self.pending_order = PendingOrder(direction, stake, score, candle_epoch)
        self.journal.log_signal(
            symbol=self.config.symbol,
            candle_epoch=candle_epoch,
            direction=direction,
            score=score,
            stake=stake,
            dry_run=self.config.dry_run,
        )

        if self.config.dry_run:
            logger.info(
                "DRY_RUN sinal=%s score=%s stake=%.2f candle=%s. Nenhuma ordem enviada.",
                direction,
                score,
                stake,
                candle_epoch,
            )
            self.last_trade_candle_epoch = candle_epoch
            self.pending_order = None
            return

        logger.info(
            "Solicitando proposta %s | stake=%.2f | ativo=%s | candle=%s",
            direction,
            stake,
            self.config.symbol,
            candle_epoch,
        )
        await self.send(
            ws,
            {
                "proposal": 1,
                "amount": stake,
                "basis": "stake",
                "contract_type": direction,
                "currency": self.config.currency,
                "duration": self.config.duration,
                "duration_unit": self.config.duration_unit,
                "symbol": self.config.symbol,
            },
        )

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
        logger.info(
            "Comprando contrato %s | proposal_id=%s | price=%s",
            self.pending_order.direction,
            proposal_id,
            ask_price,
        )
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

    def _append_or_update_candle(self, candle: dict[str, Any]) -> bool:
        epoch = int(candle["epoch"])
        normalized = {
            "epoch": epoch,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
        }

        if self.candle_buffer and int(self.candle_buffer[-1]["epoch"]) == epoch:
            self.candle_buffer[-1] = normalized
            return False

        self.candle_buffer.append(normalized)
        return True

    def _closed_candles_for_signal(self) -> list[dict[str, Any]]:
        candles = list(self.candle_buffer)
        if len(candles) < 2:
            return []
        return candles[:-1]

    async def evaluate_closed_candle(self, ws: websockets.WebSocketClientProtocol) -> None:
        if self.waiting_for_result:
            logger.info("Aguardando resultado da operacao anterior.")
            return

        if self.pending_order:
            logger.info("Aguardando proposta/compra pendente.")
            return

        if not self.risk:
            logger.warning("RiskManager ainda nao inicializado.")
            return

        closed_candles = self._closed_candles_for_signal()
        if not closed_candles:
            return

        closed_epoch = int(closed_candles[-1]["epoch"])
        if self.last_evaluated_epoch == closed_epoch:
            return

        self.last_evaluated_epoch = closed_epoch

        closed_hour = datetime.fromtimestamp(closed_epoch, UTC).hour
        if closed_hour in self.config.blocked_utc_hours:
            logger.info("Hora UTC bloqueada para novas entradas: %s", closed_hour)
            return

        if self.last_trade_candle_epoch is not None:
            candles_since_trade = (closed_epoch - self.last_trade_candle_epoch) // self.config.granularity
            if candles_since_trade <= self.config.cooldown_candles:
                logger.info(
                    "Cooldown ativo: %s candle(s) desde a ultima entrada; minimo=%s.",
                    candles_since_trade,
                    self.config.cooldown_candles + 1,
                )
                return

        if not self.risk.can_trade():
            logger.warning("Bot pausado por regra de risco.")
            return

        df = calculate_indicators(closed_candles)
        signal, score = generate_signal(df, config=self.config.strategy_config)

        if not signal:
            logger.info("Sem sinal no candle fechado %s.", closed_epoch)
            return

        stake = self.risk.get_stake()
        logger.info("Sinal detectado: %s score=%s stake=%.2f", signal, score, stake)
        await self.request_proposal(ws, signal, stake, score, closed_epoch)

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
        await self.subscribe_candles(ws)

    async def handle_candles(self, data: dict[str, Any]) -> None:
        for candle in data.get("candles", []):
            self._append_or_update_candle(candle)
        logger.info("%s candles carregados.", len(self.candle_buffer))

    async def handle_ohlc(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
        ohlc = data["ohlc"]
        is_new_candle = self._append_or_update_candle(
            {
                "epoch": ohlc["open_time"],
                "open": ohlc["open"],
                "high": ohlc["high"],
                "low": ohlc["low"],
                "close": ohlc["close"],
            }
        )

        if is_new_candle:
            await self.evaluate_closed_candle(ws)

    async def handle_buy(self, ws: websockets.WebSocketClientProtocol, data: dict[str, Any]) -> None:
        buy = data["buy"]
        contract_id = int(buy["contract_id"])
        self.current_contract_id = contract_id
        if self.pending_order:
            self.last_trade_candle_epoch = self.pending_order.candle_epoch
        logger.info("Contrato aberto: id=%s buy_price=%s", contract_id, buy.get("buy_price"))
        await self.subscribe_contract(ws, contract_id)

    def handle_contract_update(self, data: dict[str, Any]) -> None:
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
            return

        if contract_id in self.settled_contract_ids:
            return

        self.settled_contract_ids.add(contract_id)
        order = self.pending_order
        profit = float(contract.get("profit", 0.0))
        buy_price = float(contract.get("buy_price", 0.0))
        if order:
            self.journal.log_trade(
                symbol=self.config.symbol,
                contract_id=contract_id,
                candle_epoch=order.candle_epoch,
                direction=order.direction,
                score=order.score,
                stake=order.stake,
                buy_price=buy_price,
                profit=profit,
            )
        self.risk.update(profit=profit, buy_price=buy_price)
        logger.info(self.risk.stats())

        self.waiting_for_result = False
        self.current_contract_id = None
        self.pending_order = None

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
            return

        msg_type = data.get("msg_type")
        if msg_type == "authorize":
            await self.handle_authorize(ws, data)
        elif msg_type == "candles":
            await self.handle_candles(data)
        elif msg_type == "ohlc":
            await self.handle_ohlc(ws, data)
        elif msg_type == "proposal":
            await self.buy_from_proposal(ws, data["proposal"])
        elif msg_type == "buy":
            await self.handle_buy(ws, data)
        elif msg_type == "proposal_open_contract":
            self.handle_contract_update(data)
        elif msg_type == "ping":
            logger.debug("Ping recebido.")
        else:
            logger.debug("Mensagem ignorada: %s", msg_type)

    async def run_forever(self) -> None:
        while True:
            try:
                logger.info(
                    "Iniciando %s | ativo=%s | endpoint=%s",
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
