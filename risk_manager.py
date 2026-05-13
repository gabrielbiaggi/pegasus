import json
from datetime import date
from pathlib import Path

from logger import logger


class RiskManager:
    def __init__(
        self,
        balance: float,
        max_loss_day: float,
        max_profit_day: float,
        max_trades_day: int,
        max_stake_pct: float,
        fixed_stake: float,
        min_stake: float,
        max_stake: float,
        max_consecutive_losses: int,
        state_path: str = "logs/risk_state.json",
    ):
        self.balance = float(balance)
        self.max_loss_day = float(max_loss_day)
        self.max_profit_day = float(max_profit_day)
        self.max_trades_day = int(max_trades_day)
        self.max_stake_pct = float(max_stake_pct)
        self.fixed_stake = float(fixed_stake)
        self.min_stake = float(min_stake)
        self.max_stake = float(max_stake)
        self.max_consecutive_losses = int(max_consecutive_losses)
        self.state_path = Path(state_path)

        self.day = date.today().isoformat()
        self.daily_loss = 0.0
        self.daily_net_profit = 0.0
        self.trades_today = 0
        self.wins = 0
        self.losses = 0
        self.consecutive_losses = 0
        self.max_loss_streak_today = 0

        self._load_state()

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return

        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Nao foi possivel carregar estado de risco: %s", exc)
            return

        if data.get("day") != self.day:
            return

        self.daily_loss = float(data.get("daily_loss", 0.0))
        self.daily_net_profit = float(data.get("daily_net_profit", data.get("daily_profit", 0.0)))
        self.trades_today = int(data.get("trades_today", 0))
        self.wins = int(data.get("wins", 0))
        self.losses = int(data.get("losses", 0))
        self.consecutive_losses = int(data.get("consecutive_losses", 0))
        self.max_loss_streak_today = int(data.get("max_loss_streak_today", 0))
        logger.info(
            "Estado de risco restaurado: perda_dia=%.2f, lucro_liquido_dia=%.2f, trades=%s, streak_loss=%s",
            self.daily_loss,
            self.daily_net_profit,
            self.trades_today,
            self.consecutive_losses,
        )

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "day": self.day,
            "daily_loss": self.daily_loss,
            "daily_net_profit": self.daily_net_profit,
            "trades_today": self.trades_today,
            "wins": self.wins,
            "losses": self.losses,
            "consecutive_losses": self.consecutive_losses,
            "max_loss_streak_today": self.max_loss_streak_today,
        }
        self.state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _reset_if_new_day(self) -> None:
        today = date.today().isoformat()
        if today == self.day:
            return

        logger.info("Novo dia detectado. Zerando contadores diarios de risco.")
        self.day = today
        self.daily_loss = 0.0
        self.daily_net_profit = 0.0
        self.trades_today = 0
        self.wins = 0
        self.losses = 0
        self.consecutive_losses = 0
        self.max_loss_streak_today = 0
        self._save_state()

    def get_stake(self) -> float:
        self._reset_if_new_day()
        pct_cap = self.balance * self.max_stake_pct
        stake = min(self.fixed_stake, pct_cap, self.max_stake)

        if stake < self.min_stake:
            return 0.0

        return round(stake, 2)

    def can_trade(self) -> bool:
        self._reset_if_new_day()

        if self.daily_loss >= self.max_loss_day:
            logger.warning("Limite de perda diaria atingido: %.2f", self.daily_loss)
            return False

        if self.max_profit_day > 0 and self.daily_net_profit >= self.max_profit_day:
            logger.warning("Meta de lucro diaria atingida: %.2f", self.daily_net_profit)
            return False

        if self.trades_today >= self.max_trades_day:
            logger.warning("Limite diario de operacoes atingido: %s", self.trades_today)
            return False

        if self.consecutive_losses >= self.max_consecutive_losses:
            logger.warning("Limite de losses consecutivos atingido: %s", self.consecutive_losses)
            return False

        stake = self.get_stake()
        if stake <= 0:
            logger.warning(
                "Stake bloqueado: saldo %.2f com limite %.2f%% fica abaixo do minimo %.2f",
                self.balance,
                self.max_stake_pct * 100,
                self.min_stake,
            )
            return False

        if self.daily_loss + stake > self.max_loss_day:
            logger.warning(
                "Proxima stake %.2f excederia limite diario restante %.2f",
                stake,
                max(0.0, self.max_loss_day - self.daily_loss),
            )
            return False

        return True

    def update(self, profit: float, buy_price: float) -> None:
        self._reset_if_new_day()

        profit = float(profit)
        buy_price = float(buy_price)
        self.trades_today += 1
        self.balance += profit
        self.daily_net_profit += profit

        if profit > 0:
            self.wins += 1
            self.consecutive_losses = 0
            logger.info("WIN %+0.2f | saldo_estimado=%0.2f", profit, self.balance)
        else:
            self.losses += 1
            self.consecutive_losses += 1
            self.max_loss_streak_today = max(self.max_loss_streak_today, self.consecutive_losses)
            realized_loss = abs(profit) if profit < 0 else buy_price
            self.daily_loss += realized_loss
            logger.info(
                "LOSS -%0.2f | saldo_estimado=%0.2f | perda_dia=%0.2f",
                realized_loss,
                self.balance,
                self.daily_loss,
            )

        self._save_state()

    def stats(self) -> str:
        winrate = (self.wins / self.trades_today * 100) if self.trades_today else 0.0
        return (
            f"Operacoes={self.trades_today} | Wins={self.wins} | Losses={self.losses} | "
            f"WinRate={winrate:.1f}% | LossStreak={self.consecutive_losses} | "
            f"PerdaDia={self.daily_loss:.2f} | LucroLiquidoDia={self.daily_net_profit:.2f} | "
            f"SaldoEstimado={self.balance:.2f}"
        )
