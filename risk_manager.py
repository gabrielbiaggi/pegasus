import json
import time
from collections import deque
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
        daily_trailing_start: float,
        daily_trailing_lock: float,
        max_stake_pct: float,
        fixed_stake: float,
        min_stake: float,
        max_stake: float,
        max_consecutive_losses: int,
        use_soros: bool,
        soros_max_steps: int,
        soros_profit_factor: float,
        use_dynamic_stake: bool = True,
        dynamic_stake_base_pct: float = 0.02,
        use_martingale: bool = False,
        martingale_max_gales: int = 3,
        martingale_multiplier: float = 2.0,  # deprecated: formula now usa payout_rate
        martingale_payout_rate: float = 0.15,
        state_path: str = "logs/risk_state.json",
        max_losses_in_window: int = 2,
        loss_window_seconds: float = 300.0,
    ):
        self.balance = float(balance)
        self.max_loss_day = float(max_loss_day)
        self.max_profit_day = float(max_profit_day)
        self.max_trades_day = int(max_trades_day)
        self.daily_trailing_start = float(daily_trailing_start)
        self.daily_trailing_lock = float(daily_trailing_lock)
        self.max_stake_pct = float(max_stake_pct)
        self.fixed_stake = float(fixed_stake)
        self.min_stake = float(min_stake)
        self.max_stake = float(max_stake)
        self.max_consecutive_losses = int(max_consecutive_losses)
        self.use_soros = bool(use_soros)
        self.soros_max_steps = int(soros_max_steps)
        self.soros_profit_factor = float(soros_profit_factor)
        self.use_dynamic_stake = bool(use_dynamic_stake)
        self.dynamic_stake_base_pct = float(dynamic_stake_base_pct)
        self.use_martingale = bool(use_martingale)
        self.martingale_max_gales = int(martingale_max_gales)
        self.martingale_multiplier = float(martingale_multiplier)
        self.martingale_payout_rate = max(0.001, float(martingale_payout_rate))
        self.state_path = Path(state_path)
        self.max_losses_in_window = int(max_losses_in_window)
        self.loss_window_seconds = float(loss_window_seconds)

        self.day = date.today().isoformat()
        self.daily_loss = 0.0
        self.daily_net_profit = 0.0
        self.daily_peak_profit = 0.0
        self.daily_trailing_active = False
        self.trades_today = 0
        self.wins = 0
        self.losses = 0
        self.consecutive_losses = 0
        self.max_loss_streak_today = 0
        self.loss_block_override = False   # set True via dashboard to bypass daily loss limit
        self.soros_step = 0
        self.soros_profit = 0.0
        self.martingale_step = 0
        self.martingale_accumulated_loss: float = 0.0  # soma dos buy_prices perdidos na sequencia atual
        self.martingale_base_stake: float = 0.0       # stake da primeira aposta da sequencia (step 0)
        # Sliding window timestamps of recent losses (monotonic clock)
        self._recent_loss_times: deque[float] = deque()
        # Last time we re-read the state file to pick up external overrides (e.g. dashboard unblock)
        self._last_override_check: float = 0.0

        self._load_state()

    def _reload_overrides(self) -> None:
        """Re-read only the override/block fields from disk.

        Called by can_trade() when the bot is in a blocked state so that an
        external unblock (e.g. dashboard setting loss_block_override=True or
        resetting consecutive_losses) is detected without requiring a restart.
        Rate-limited to at most once every 5 seconds.
        """
        now = time.monotonic()
        if now - self._last_override_check < 5.0:
            return
        self._last_override_check = now

        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return

        if data.get("day") != self.day:
            return

        new_override = bool(data.get("loss_block_override", False))
        new_consec = int(data.get("consecutive_losses", self.consecutive_losses))
        if new_override != self.loss_block_override or new_consec != self.consecutive_losses:
            logger.info(
                "Override detectado no disco: loss_block_override %s→%s  consecutive_losses %s→%s",
                self.loss_block_override, new_override,
                self.consecutive_losses, new_consec,
            )
            self.loss_block_override = new_override
            self.consecutive_losses = new_consec

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
        self.daily_peak_profit = float(data.get("daily_peak_profit", max(0.0, self.daily_net_profit)))
        self.daily_trailing_active = bool(data.get("daily_trailing_active", False))
        self.trades_today = int(data.get("trades_today", 0))
        self.wins = int(data.get("wins", 0))
        self.losses = int(data.get("losses", 0))
        self.consecutive_losses = int(data.get("consecutive_losses", 0))
        self.max_loss_streak_today = int(data.get("max_loss_streak_today", 0))
        self.soros_step = int(data.get("soros_step", 0))
        self.soros_profit = float(data.get("soros_profit", 0.0))
        self.martingale_step = int(data.get("martingale_step", 0))
        self.martingale_accumulated_loss = float(data.get("martingale_accumulated_loss", 0.0))
        self.martingale_base_stake = float(data.get("martingale_base_stake", 0.0))
        self.loss_block_override = bool(data.get("loss_block_override", False))
        # Segurança: estado legado sem base_stake → reseta gale (melhor pausar que calcular errado)
        if self.martingale_step > 0 and self.martingale_base_stake == 0.0:
            logger.warning("Estado martingale inconsistente (base_stake=0 com step=%d). Resetando gale.", self.martingale_step)
            self.martingale_step = 0
            self.martingale_accumulated_loss = 0.0
        self.max_loss_day = float(data.get("max_loss_day", self.max_loss_day))
        logger.info(
            "Estado de risco restaurado: perda_dia=%.2f, lucro_liquido_dia=%.2f, "
            "trailing=%s, trades=%s, streak_loss=%s",
            self.daily_loss,
            self.daily_net_profit,
            self.daily_trailing_active,
            self.trades_today,
            self.consecutive_losses,
        )

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "day": self.day,
            "max_loss_day": self.max_loss_day,
            "daily_loss": self.daily_loss,
            "daily_net_profit": self.daily_net_profit,
            "daily_peak_profit": self.daily_peak_profit,
            "daily_trailing_active": self.daily_trailing_active,
            "trades_today": self.trades_today,
            "wins": self.wins,
            "losses": self.losses,
            "consecutive_losses": self.consecutive_losses,
            "max_loss_streak_today": self.max_loss_streak_today,
            "soros_step": self.soros_step,
            "soros_profit": self.soros_profit,
            "martingale_step": self.martingale_step,
            "martingale_accumulated_loss": self.martingale_accumulated_loss,
            "martingale_base_stake": self.martingale_base_stake,
            "loss_block_override": self.loss_block_override,
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
        self.daily_peak_profit = 0.0
        self.daily_trailing_active = False
        self.trades_today = 0
        self.wins = 0
        self.losses = 0
        self.consecutive_losses = 0
        self.max_loss_streak_today = 0
        self.loss_block_override = False
        self.soros_step = 0
        self.soros_profit = 0.0
        self.martingale_step = 0
        self.martingale_accumulated_loss = 0.0
        self.martingale_base_stake = 0.0
        self._save_state()

    def abandon_gale(self) -> None:
        """Abandona a sequência de gale atual — absorve as perdas acumuladas e reseta para G0."""
        logger.warning(
            "⛔ Gale %d/%d ABANDONADO por P(LOSS) alto — perdas absorvidas: %.2f",
            self.martingale_step,
            self.martingale_max_gales,
            self.martingale_accumulated_loss,
        )
        self.martingale_step = 0
        self.martingale_accumulated_loss = 0.0
        self.martingale_base_stake = 0.0
        self._save_state()

    def get_stake(self, p_loss: float | None = None) -> float:
        self._reset_if_new_day()

        if self.use_dynamic_stake and p_loss is not None:
            # Base = % da banca atual (nunca menor que fixed_stake mínimo)
            base = max(self.balance * self.dynamic_stake_base_pct, self.fixed_stake)

            # Tabela de multiplicadores por confiança da IA (P(LOSS) baixo = IA confiante)
            if p_loss < 0.05:
                raw_stake = base * 4.0    # confiança máxima: 4× banca_base
            elif p_loss < 0.10:
                raw_stake = base * 3.0    # muito confiante: 3×
            elif p_loss < 0.15:
                raw_stake = base * 2.0    # confiante: 2×
            elif p_loss < 0.20:
                raw_stake = base * 1.5    # moderado: 1.5×
            elif p_loss < 0.25:
                raw_stake = base * 1.25   # limiar próximo: 1.25×
            else:
                raw_stake = base          # p_loss próximo ao limite: 1× base
        else:
            # Sem IA ou dynamic stake off: aposta fixa conservadora
            raw_stake = self.fixed_stake

        # Soros: reinveste lucro acumulado de wins consecutivos (limpos, não gale)
        if self.use_soros and 0 < self.soros_step <= self.soros_max_steps and self.soros_profit > 0:
            raw_stake = raw_stake + self.soros_profit

        # Gale-safe cap: limita G0 (incluindo Soros) para que G(max_gales) caiba dentro de max_stake.
        # Posicionado APÓS Soros para capturar qualquer inflação de stake.
        # Fórmula: G_n = G0 × (1 + 1/rate)^n  →  G0_max = max_stake / (1+1/rate)^n
        # Com rate=0.50 e max_gales=2: G0_max = max_stake / 9
        # Garante recuperação total no último gale SEM estourar o cap.
        if (
            self.use_martingale
            and self.martingale_step == 0
            and self.max_stake > 0
            and self.martingale_payout_rate > 0
        ):
            gale_factor = (1.0 + 1.0 / self.martingale_payout_rate) ** self.martingale_max_gales
            raw_stake = min(raw_stake, self.max_stake / gale_factor)

        # Martingale: calcula stake de recuperacao matematicamente correta para o payout atual.
        # Formula: G = perdas_acumuladas / payout_rate + stake_base
        # Garante que WIN no proximo gale recupera todas as perdas anteriores + lucro original.
        # Exemplo com payout=15%: gale1 = loss0/0.15 + loss0 ≈ 7.67× (NAO 2×!)
        if self.use_martingale and self.martingale_step > 0 and self.martingale_base_stake > 0:
            raw_stake = self.martingale_accumulated_loss / self.martingale_payout_rate + self.martingale_base_stake

        remaining_budget = max(0.0, self.max_loss_day + self.daily_net_profit)

        if self.use_martingale and self.martingale_step > 0 and self.martingale_base_stake > 0:
            # Martingale recovery: o limite é a banca inteira.
            caps = [raw_stake, self.balance]
        elif self.loss_block_override:
            # Override ativo: ignora o budget restante do limite diário, usa banca inteira.
            caps = [raw_stake, self.balance]
        else:
            # Cap = banca inteira (sem pct_cap). Proteção real via can_trade() e remaining_budget.
            caps = [raw_stake, remaining_budget, self.balance]
        if self.max_stake > 0:
            caps.append(self.max_stake)
        stake = min(caps)

        if stake < self.min_stake:
            return 0.0

        return round(stake, 2)

    def get_gale_raw_stake(self) -> float:
        """Returns the uncapped martingale recovery stake (may exceed max_stake API limit).

        Use this to detect when the gale needs to be split into multiple simultaneous
        contracts to work around Deriv's per-contract stake ceiling.
        Returns 0.0 when not in a martingale gale.
        """
        if not (self.use_martingale and self.martingale_step > 0 and self.martingale_base_stake > 0):
            return 0.0
        raw = self.martingale_accumulated_loss / self.martingale_payout_rate + self.martingale_base_stake
        return round(min(raw, self.balance), 2)

    def can_trade(self) -> bool:
        self._reset_if_new_day()
        # Check for external override changes written by dashboard (no restart needed)
        self._reload_overrides()

        if self.daily_net_profit <= -self.max_loss_day and not self.loss_block_override:
            logger.warning(
                "Stop loss diario atingido: lucro_liquido=%.2f (limite=-%.2f)",
                self.daily_net_profit,
                self.max_loss_day,
            )
            return False

        if self.max_profit_day > 0 and self.daily_net_profit >= self.max_profit_day:
            logger.warning("Meta de lucro diaria atingida: %.2f", self.daily_net_profit)
            return False

        if self.daily_trailing_active and self.daily_net_profit <= self.daily_trailing_lock:
            logger.warning(
                "Trailing diario protegido: lucro_liquido=%.2f lock=%.2f",
                self.daily_net_profit,
                self.daily_trailing_lock,
            )
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

        # NOTE: daily_loss + stake never exceeds max_loss_day because get_stake()
        # already caps stake by (max_loss_day - daily_loss). No need to re-check here.

        # Protect trailing lock: don't enter a trade that would risk dropping below the locked profit
        if self.daily_trailing_active and self.daily_net_profit - stake < self.daily_trailing_lock:
            logger.warning(
                "Proxima stake %.2f arriscaria lucro protegido %.2f (lock=%.2f)",
                stake,
                self.daily_net_profit,
                self.daily_trailing_lock,
            )
            return False

        # Frequency-based drawdown: stop if too many losses in sliding time window
        now = time.monotonic()
        cutoff = now - self.loss_window_seconds
        while self._recent_loss_times and self._recent_loss_times[0] < cutoff:
            self._recent_loss_times.popleft()
        if len(self._recent_loss_times) >= self.max_losses_in_window:
            logger.warning(
                "Frequencia de losses excedida: %d losses nos ultimos %.0fs (limite=%d). Bot pausado.",
                len(self._recent_loss_times),
                self.loss_window_seconds,
                self.max_losses_in_window,
            )
            return False

        return True

    def update(self, profit: float, buy_price: float) -> None:
        self._reset_if_new_day()

        profit = float(profit)
        buy_price = float(buy_price)
        self.trades_today += 1
        # NOTE: self.balance is updated exclusively by the Deriv balance stream
        # (bot.py balance message handler). Do NOT add profit here — Deriv already
        # sends the confirmed post-trade balance via the balance subscription,
        # so incrementing here would double-count every win/loss.
        self.daily_net_profit += profit
        self.daily_peak_profit = max(self.daily_peak_profit, self.daily_net_profit)

        if self.daily_trailing_start > 0 and self.daily_peak_profit >= self.daily_trailing_start:
            self.daily_trailing_active = True

        if profit > 0:
            self.wins += 1
            self.consecutive_losses = 0
            _was_gale_win = self.use_martingale and self.martingale_step > 0
            if self.use_martingale and self.martingale_step > 0:
                # Partial recovery: reduce accumulated_loss by this WIN's profit.
                # The gale stake may have been capped by MAX_STAKE so a single WIN
                # might not cover all previous losses. Keep gale active until fully covered.
                self.martingale_accumulated_loss = round(
                    max(0.0, self.martingale_accumulated_loss - profit), 2
                )
                if self.martingale_accumulated_loss <= 0.0:
                    logger.info(
                        "\u2705 Martingale recuperado | step=%d \u2192 0 | todas perdas cobertas",
                        self.martingale_step,
                    )
                    self.martingale_step = 0
                    self.martingale_accumulated_loss = 0.0
                    self.martingale_base_stake = 0.0
                elif self.martingale_step >= self.martingale_max_gales:
                    # Circuit breaker: reached max_gales — absorb residual and reset.
                    logger.warning(
                        "\u26a0 Martingale max_gales=%d atingido com residuo=%.2f — absorvido como perda aceita",
                        self.martingale_max_gales, self.martingale_accumulated_loss,
                    )
                    self.martingale_step = 0
                    self.martingale_accumulated_loss = 0.0
                    self.martingale_base_stake = 0.0
                else:
                    logger.info(
                        "\u26a1 Martingale parcial: +%.2f recuperado | residual=%.2f | stake_prox=%.2f",
                        profit,
                        self.martingale_accumulated_loss,
                        round(self.martingale_accumulated_loss / self.martingale_payout_rate + self.martingale_base_stake, 2),
                    )
            else:
                self.martingale_step = 0
                self.martingale_accumulated_loss = 0.0
                self.martingale_base_stake = 0.0
            # Soros só ativa em wins limpos (G0), NÃO em recuperações de gale.
            # Após um gale win, o "lucro" bruto é apenas recuperação de perdas, não ganho real.
            if self.use_soros and self.soros_max_steps > 0 and not _was_gale_win:
                if self.soros_step < self.soros_max_steps:
                    self.soros_step += 1
                    # Acumula o lucro de todos os wins da sequência Soros (não sobrescreve)
                    self.soros_profit = round(self.soros_profit + profit * self.soros_profit_factor, 2)
                else:
                    self.soros_step = 0
                    self.soros_profit = 0.0
            _modo_win = f"SOROS {self.soros_step}/{self.soros_max_steps}" if self.use_soros and self.soros_step > 0 else "NORMAL"
            logger.info("WIN %+0.2f | saldo_estimado=%0.2f | modo=%s", profit, self.balance, _modo_win)
        else:
            self.losses += 1
            self.consecutive_losses += 1
            self.soros_step = 0
            self.soros_profit = 0.0
            if self.use_martingale:
                if self.martingale_step == 0:
                    # Primeiro loss: guarda a stake base desta sequencia de gales
                    self.martingale_base_stake = buy_price
                self.martingale_accumulated_loss += buy_price
                if self.martingale_step >= self.martingale_max_gales:
                    # Último gale esgotado — perdas absorvidas, reseta sequência para G0
                    logger.warning(
                        "⛔ Gale %d/%d ESGOTADO — perdas totais=%.2f absorvidas, iniciando nova sequência",
                        self.martingale_step,
                        self.martingale_max_gales,
                        self.martingale_accumulated_loss,
                    )
                    self.martingale_step = 0
                    self.martingale_accumulated_loss = 0.0
                    self.martingale_base_stake = 0.0
                else:
                    self.martingale_step += 1
            self.max_loss_streak_today = max(self.max_loss_streak_today, self.consecutive_losses)
            realized_loss = abs(profit) if profit < 0 else buy_price
            self.daily_loss += realized_loss
            # Record timestamp for frequency-based MDD
            self._recent_loss_times.append(time.monotonic())
            _modo_loss = f"GALE {self.martingale_step}/{self.martingale_max_gales}" if self.use_martingale and self.martingale_step > 0 else "NORMAL"
            logger.info(
                "LOSS -%0.2f | saldo_estimado=%0.2f | perda_dia=%0.2f | modo=%s",
                realized_loss,
                self.balance,
                self.daily_loss,
                _modo_loss,
            )

        self._save_state()

    def stats(self) -> str:
        winrate = (self.wins / self.trades_today * 100) if self.trades_today else 0.0
        return (
            f"Operacoes={self.trades_today} | Wins={self.wins} | Losses={self.losses} | "
            f"WinRate={winrate:.1f}% | LossStreak={self.consecutive_losses} | "
            f"PerdaDia={self.daily_loss:.2f} | LucroLiquidoDia={self.daily_net_profit:.2f} | "
            f"TrailingAtivo={self.daily_trailing_active} | SorosStep={self.soros_step} | "
            f"MartingaleStep={self.martingale_step} | "
            f"SaldoEstimado={self.balance:.2f}"
        )
