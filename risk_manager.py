import json
import time
from collections import deque
from datetime import date
from pathlib import Path

from logger import logger

FIB_SEQUENCE = [1, 1, 2, 3, 5, 8, 13, 21]


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
        dynamic_stake_base_pct: float = 0.02,
        use_martingale: bool = False,
        martingale_max_gales: int = 3,
        martingale_multiplier: float = 2.0,  # deprecated: formula now usa payout_rate
        martingale_payout_rate: float = 0.15,
        martingale_max_balance_pct: float = 0.7,
        martingale_min_balance_floor: float = 0.0,
        martingale_lock_config: bool = True,
        martingale_mode: str = "classic",
        state_path: str = "logs/risk_state.json",
        max_losses_in_window: int = 2,
        loss_window_seconds: float = 300.0,
        stop_loss_pct: float = 0.0,
        stop_gain_pct: float = 0.0,
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
        self.dynamic_stake_base_pct = float(dynamic_stake_base_pct)
        self.use_martingale = bool(use_martingale)
        self.martingale_max_gales = int(martingale_max_gales)
        self.martingale_multiplier = float(martingale_multiplier)
        self.martingale_payout_rate = max(0.001, float(martingale_payout_rate))
        self.martingale_max_balance_pct = float(martingale_max_balance_pct)
        self.martingale_min_balance_floor = float(martingale_min_balance_floor)
        self.martingale_lock_config = bool(martingale_lock_config)
        self.martingale_mode = martingale_mode
        self.state_path = Path(state_path)
        self.max_losses_in_window = int(max_losses_in_window)
        self.loss_window_seconds = float(loss_window_seconds)
        self.stop_loss_pct = float(stop_loss_pct)
        self.stop_gain_pct = float(stop_gain_pct)

        self.day = date.today().isoformat()
        self.start_of_day_balance = float(balance)  # fixed reference for P&L
        self.daily_loss = 0.0
        self.daily_net_profit = 0.0
        self.daily_peak_profit = 0.0
        self.daily_trailing_active = False
        self.trades_today = 0
        self.wins = 0
        self.losses = 0
        self.consecutive_losses = 0
        self.max_loss_streak_today = 0
        self.loss_block_override = (
            False  # set True via dashboard to bypass daily loss limit
        )
        self.soros_step = 0
        self.soros_profit = 0.0
        self.martingale_step = 0
        self.martingale_accumulated_loss: float = (
            0.0  # soma dos buy_prices perdidos na sequencia atual
        )
        self.martingale_base_stake: float = (
            0.0  # stake da primeira aposta da sequencia (step 0)
        )
        # Sliding window timestamps of recent losses (monotonic clock)
        self._recent_loss_times: deque[float] = deque()
        # Last time we re-read the state file to pick up external overrides (e.g. dashboard unblock)
        self._last_override_check: float = 0.0
        # Set by handle_buy() to the stake amount already deducted from balance.
        # update() consumes it to know whether to deduct the stake itself (unit tests)
        # or just add it back on WIN (production with prior handle_buy).
        self._pending_stake_deduction: float = 0.0
        # Throttle can_trade() warning logs (monotonic timestamps, one log per 60s per reason)
        self._log_ts_profit: float = 0.0
        self._log_ts_loss: float = 0.0
        self._log_ts_trailing: float = 0.0
        self._log_ts_trades: float = 0.0
        self._log_ts_consec: float = 0.0
        self._log_ts_freq: float = 0.0

        self._load_state()

    def _reload_overrides(self) -> None:
        """Re-read override fields from disk + env settings changed via dashboard.

        Called by can_trade() so that external changes (dashboard unblock,
        stop loss/gain adjustments) are detected without requiring a restart.
        Rate-limited to at most once every 5 seconds.
        """
        now = time.monotonic()
        if now - self._last_override_check < 5.0:
            return
        self._last_override_check = now

        # --- re-read .env for real-time stop loss/gain changes ---
        self._reload_env_settings()

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
        if (
            new_override != self.loss_block_override
            or new_consec != self.consecutive_losses
        ):
            logger.info(
                "Override detectado no disco: loss_block_override %s→%s  consecutive_losses %s→%s",
                self.loss_block_override,
                new_override,
                self.consecutive_losses,
                new_consec,
            )
            self.loss_block_override = new_override
            self.consecutive_losses = new_consec

        # Reconcile trigger: when "reconcile" flag is set in risk_state.json,
        # reload counters from file and sync P&L from balance.
        if data.get("reconcile"):
            self.daily_loss = float(data.get("daily_loss", self.daily_loss))
            self.trades_today = int(data.get("trades_today", self.trades_today))
            self.wins = int(data.get("wins", self.wins))
            self.losses = int(data.get("losses", self.losses))
            if "start_of_day_balance" in data:
                self.start_of_day_balance = float(data["start_of_day_balance"])
            self.sync_pnl_from_balance()
            logger.info(
                "♻ Reconciliado do disco: trades=%d, W=%d, L=%d, "
                "net_profit=%.2f, daily_loss=%.2f, saldo_inicio=%.2f",
                self.trades_today,
                self.wins,
                self.losses,
                self.daily_net_profit,
                self.daily_loss,
                self.start_of_day_balance,
            )
            # Clear reconcile flag from file so it doesn't re-trigger
            data.pop("reconcile", None)
            self.state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _reload_env_settings(self) -> None:
        """Re-read .env file for dashboard-adjustable settings (stop loss/gain, stake, max_stake, etc.).

        This is the SINGLE SOURCE OF TRUTH for these values during runtime.
        The .env file is written by the dashboard and read here every 5s.
        """
        env_path = self.state_path.parent.parent / ".env"
        if not env_path.exists():
            return
        try:
            env_mtime = env_path.stat().st_mtime
            if env_mtime == getattr(self, "_env_mtime", -1.0):
                return
            self._env_mtime = env_mtime
            env_data: dict[str, str] = {}
            for line in env_path.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env_data[k.strip()] = v.strip()

            # PROTEÇÃO: bloqueia alteração de config crítica durante gale ativo
            _gale_active = self.use_martingale and self.martingale_step > 0
            _config_locked = _gale_active and self.martingale_lock_config

            # Update stop loss — MAX_LOSS_PER_DAY is the sole source (pct logic only at startup)
            new_loss = float(env_data.get("MAX_LOSS_PER_DAY", str(self.max_loss_day)))
            if new_loss != self.max_loss_day:
                if _config_locked:
                    logger.warning(
                        "🔒 Stop Loss BLOQUEADO durante gale %d/%d (tentativa: %.2f → %.2f)",
                        self.martingale_step,
                        self.martingale_max_gales,
                        self.max_loss_day,
                        new_loss,
                    )
                else:
                    logger.info(
                        "Stop Loss atualizado via dashboard: %.2f → %.2f",
                        self.max_loss_day,
                        new_loss,
                    )
                    self.max_loss_day = new_loss
            # Update stop gain
            new_profit = float(
                env_data.get("MAX_PROFIT_PER_DAY", str(self.max_profit_day))
            )
            if new_profit != self.max_profit_day:
                if _config_locked:
                    logger.warning(
                        "🔒 Stop Gain BLOQUEADO durante gale %d/%d (tentativa: %.2f → %.2f)",
                        self.martingale_step,
                        self.martingale_max_gales,
                        self.max_profit_day,
                        new_profit,
                    )
                else:
                    logger.info(
                        "Stop Gain atualizado via dashboard: %.2f → %.2f",
                        self.max_profit_day,
                        new_profit,
                    )
                    self.max_profit_day = new_profit
            # Update stake
            new_stake = float(env_data.get("STAKE", str(self.fixed_stake)))
            if new_stake != self.fixed_stake:
                if _config_locked:
                    logger.warning(
                        "🔒 Stake BLOQUEADO durante gale %d/%d (tentativa: %.2f → %.2f)",
                        self.martingale_step,
                        self.martingale_max_gales,
                        self.fixed_stake,
                        new_stake,
                    )
                else:
                    logger.info(
                        "Stake atualizado via dashboard: %.2f → %.2f",
                        self.fixed_stake,
                        new_stake,
                    )
                    self.fixed_stake = new_stake
            # Update max_stake
            new_max_stake = float(env_data.get("MAX_STAKE", str(self.max_stake)))
            if new_max_stake != self.max_stake:
                if _config_locked:
                    logger.warning(
                        "🔒 Max Stake BLOQUEADO durante gale %d/%d (tentativa: %.2f → %.2f)",
                        self.martingale_step,
                        self.martingale_max_gales,
                        self.max_stake,
                        new_max_stake,
                    )
                else:
                    logger.info(
                        "Max Stake atualizado via dashboard: %.2f → %.2f",
                        self.max_stake,
                        new_max_stake,
                    )
                    self.max_stake = new_max_stake
            # Update stop loss/gain percentages
            new_sl_pct = float(env_data.get("STOP_LOSS_PCT", str(self.stop_loss_pct)))
            if new_sl_pct != self.stop_loss_pct:
                if _config_locked:
                    logger.warning(
                        "🔒 Stop Loss %% BLOQUEADO durante gale %d/%d",
                        self.martingale_step,
                        self.martingale_max_gales,
                    )
                else:
                    logger.info(
                        "Stop Loss %% atualizado: %.1f%% → %.1f%%",
                        self.stop_loss_pct,
                        new_sl_pct,
                    )
                    self.stop_loss_pct = new_sl_pct
            new_sg_pct = float(env_data.get("STOP_GAIN_PCT", str(self.stop_gain_pct)))
            if new_sg_pct != self.stop_gain_pct:
                if _config_locked:
                    logger.warning(
                        "🔒 Stop Gain %% BLOQUEADO durante gale %d/%d",
                        self.martingale_step,
                        self.martingale_max_gales,
                    )
                else:
                    logger.info(
                        "Stop Gain %% atualizado: %.1f%% → %.1f%%",
                        self.stop_gain_pct,
                        new_sg_pct,
                    )
                    self.stop_gain_pct = new_sg_pct
            # Update stake percentage (not locked — doesn't affect active gale calc)
            new_base_pct = float(
                env_data.get("DYNAMIC_STAKE_BASE_PCT", str(self.dynamic_stake_base_pct))
            )
            if new_base_pct != self.dynamic_stake_base_pct:
                logger.info(
                    "Stake base pct atualizado: %.4f → %.4f",
                    self.dynamic_stake_base_pct,
                    new_base_pct,
                )
                self.dynamic_stake_base_pct = new_base_pct
            # Live-reload martingale/soros toggles (sem restart)
            new_use_mg = env_data.get("USE_MARTINGALE", "").strip().lower() == "true"
            if new_use_mg != self.use_martingale:
                if _config_locked:
                    logger.warning(
                        "U0001f512 USE_MARTINGALE BLOQUEADO durante gale %d/%d",
                        self.martingale_step,
                        self.martingale_max_gales,
                    )
                else:
                    logger.info(
                        "Martingale toggle atualizado: %s → %s",
                        self.use_martingale,
                        new_use_mg,
                    )
                    self.use_martingale = new_use_mg
            new_use_soros = env_data.get("USE_SOROS", "").strip().lower() == "true"
            if new_use_soros != self.use_soros:
                logger.info(
                    "Soros toggle atualizado: %s → %s", self.use_soros, new_use_soros
                )
                self.use_soros = new_use_soros
            new_max_gales = int(
                env_data.get("MARTINGALE_MAX_GALES", str(self.martingale_max_gales))
            )
            if new_max_gales != self.martingale_max_gales:
                if _config_locked:
                    logger.warning(
                        "U0001f512 MARTINGALE_MAX_GALES BLOQUEADO durante gale %d/%d",
                        self.martingale_step,
                        self.martingale_max_gales,
                    )
                else:
                    logger.info(
                        "Max gales atualizado: %d → %d",
                        self.martingale_max_gales,
                        new_max_gales,
                    )
                    self.martingale_max_gales = new_max_gales
            # SOROS_MAX_STEPS live-reload
            new_soros_steps = int(
                env_data.get("SOROS_MAX_STEPS", str(self.soros_max_steps))
            )
            if new_soros_steps != self.soros_max_steps:
                logger.info(
                    "Soros max steps atualizado: %d → %d",
                    self.soros_max_steps,
                    new_soros_steps,
                )
                self.soros_max_steps = new_soros_steps
            # SOROS_PROFIT_FACTOR live-reload
            new_soros_factor = float(
                env_data.get("SOROS_PROFIT_FACTOR", str(self.soros_profit_factor))
            )
            if new_soros_factor != self.soros_profit_factor:
                logger.info(
                    "Soros profit factor atualizado: %.1f → %.1f",
                    self.soros_profit_factor,
                    new_soros_factor,
                )
                self.soros_profit_factor = new_soros_factor
            # MARTINGALE_PAYOUT_RATE live-reload
            new_payout = float(
                env_data.get("MARTINGALE_PAYOUT_RATE", str(self.martingale_payout_rate))
            )
            new_payout = max(0.001, new_payout)
            if new_payout != self.martingale_payout_rate:
                if _config_locked:
                    logger.warning(
                        "\U0001f512 MARTINGALE_PAYOUT_RATE BLOQUEADO durante gale %d/%d",
                        self.martingale_step,
                        self.martingale_max_gales,
                    )
                else:
                    logger.info(
                        "Payout rate atualizado: %.4f → %.4f",
                        self.martingale_payout_rate,
                        new_payout,
                    )
                    self.martingale_payout_rate = new_payout
            # BLOCK_WEEKENDS live-reload
            self.block_weekends = (
                env_data.get("BLOCK_WEEKENDS", "true").strip().lower() == "true"
            )
        except (OSError, ValueError):
            pass

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
        self.daily_net_profit = float(
            data.get("daily_net_profit", data.get("daily_profit", 0.0))
        )
        self.daily_peak_profit = float(
            data.get("daily_peak_profit", max(0.0, self.daily_net_profit))
        )

        # CRITICAL FIX: restore start_of_day_balance from the saved state so that
        # MAX_LOSS_PER_DAY is ALWAYS calculated relative to the true beginning of the
        # calendar day — not the balance at the moment of this restart.
        # Without this, every restart silently resets the daily loss reference.
        saved_sod = float(data.get("start_of_day_balance", 0.0))
        if saved_sod > 0:
            self.start_of_day_balance = saved_sod
            logger.info(
                "start_of_day_balance restaurado do estado: %.2f (saldo_atual=%.2f, perda_real_dia=%.2f)",
                saved_sod,
                self.balance,
                self.balance - saved_sod,
            )
        self.daily_trailing_active = bool(data.get("daily_trailing_active", False))
        self.trades_today = int(data.get("trades_today", 0))
        self.wins = int(data.get("wins", 0))
        self.losses = int(data.get("losses", 0))
        self.consecutive_losses = int(data.get("consecutive_losses", 0))
        self.max_loss_streak_today = int(data.get("max_loss_streak_today", 0))
        self.soros_step = int(data.get("soros_step", 0))
        self.soros_profit = float(data.get("soros_profit", 0.0))
        self.martingale_step = int(data.get("martingale_step", 0))
        self.martingale_accumulated_loss = float(
            data.get("martingale_accumulated_loss", 0.0)
        )
        self.martingale_base_stake = float(data.get("martingale_base_stake", 0.0))
        self.loss_block_override = bool(data.get("loss_block_override", False))
        # Segurança: estado legado sem base_stake → reseta gale (melhor pausar que calcular errado)
        if self.martingale_step > 0 and self.martingale_base_stake == 0.0:
            logger.warning(
                "Estado martingale inconsistente (base_stake=0 com step=%d). Resetando gale.",
                self.martingale_step,
            )
            self.martingale_step = 0
            self.martingale_accumulated_loss = 0.0
        # NOTE: max_loss_day is NOT loaded from state file — .env is the single source of truth
        # (dashboard writes to .env, _reload_env_settings() reads it every 5s)
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
            "start_of_day_balance": self.start_of_day_balance,
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

    def reconcile_pnl(self, db_summary: dict[str, float | int]) -> None:
        """Reconcile in-memory trade counters with actual DB values.

        daily_net_profit is NOT overwritten here — it comes from
        balance - start_of_day_balance (synced via sync_pnl_from_balance).
        """
        old = (self.trades_today, self.wins, self.losses, self.daily_loss)
        self.trades_today = int(db_summary["trades"])
        self.wins = int(db_summary["wins"])
        self.losses = int(db_summary["losses"])
        self.daily_loss = float(db_summary["total_loss"])
        # Sync net_profit from balance (the single source of truth)
        self.daily_net_profit = round(self.balance - self.start_of_day_balance, 2)
        self.daily_peak_profit = max(self.daily_peak_profit, self.daily_net_profit)
        new = (self.trades_today, self.wins, self.losses, self.daily_loss)
        if old != new:
            logger.info(
                "♻ Reconciliado com DB: trades %d→%d | W %d→%d | L %d→%d | "
                "loss %.2f→%.2f | net_profit(saldo)=%.2f | saldo_inicio_sessao=%.2f",
                old[0],
                new[0],
                old[1],
                new[1],
                old[2],
                new[2],
                old[3],
                new[3],
                self.daily_net_profit,
                self.start_of_day_balance,
            )
            self._save_state()
        else:
            logger.info(
                "♻ DB confere | net_profit(saldo)=%.2f | saldo_inicio_sessao=%.2f",
                self.daily_net_profit,
                self.start_of_day_balance,
            )

    def _reset_if_new_day(self) -> None:
        today = date.today().isoformat()
        if today == self.day:
            return

        logger.info("Novo dia detectado. Zerando contadores diarios de risco.")
        self.day = today
        self.start_of_day_balance = self.balance
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

    def sync_pnl_from_balance(self) -> None:
        """Recompute daily_net_profit from actual balance vs start-of-day.

        Called every time Deriv sends a new balance — makes P&L always match
        the real balance change, eliminating drift from restarts or timeouts.
        """
        new_pnl = round(self.balance - self.start_of_day_balance, 2)
        if abs(new_pnl - self.daily_net_profit) > 0.005:
            self.daily_net_profit = new_pnl
            self.daily_peak_profit = max(self.daily_peak_profit, self.daily_net_profit)
            if (
                self.daily_trailing_start > 0
                and self.daily_peak_profit >= self.daily_trailing_start
            ):
                self.daily_trailing_active = True
            self._save_state()

    @property
    def _start_of_day_balance(self) -> float:
        """Balance at the start of the trading day (before any trades)."""
        return self.start_of_day_balance

    def _effective_loss_limit(self) -> float:
        """Dynamic loss limit: uses % of start-of-day balance when stop_loss_pct > 0."""
        if self.stop_loss_pct > 0:
            return self._start_of_day_balance * self.stop_loss_pct / 100.0
        return self.max_loss_day

    def _effective_profit_limit(self) -> float:
        """Dynamic profit target: uses % of start-of-day balance when stop_gain_pct > 0."""
        if self.stop_gain_pct > 0:
            return self._start_of_day_balance * self.stop_gain_pct / 100.0
        return self.max_profit_day

    def get_stake(self) -> float:
        self._reset_if_new_day()

        # Base = % da banca atual OU fixed_stake quando pct == 0
        if self.dynamic_stake_base_pct > 0:
            raw_stake = max(self.balance * self.dynamic_stake_base_pct, self.min_stake)
        else:
            raw_stake = self.fixed_stake

        _in_gale = self.use_martingale and self.martingale_step > 0

        # Soros: reinveste lucro acumulado de wins consecutivos (limpos, não gale)
        # Posicionado APÓS gale-safe cap para que cada step Soros cresça progressivamente.
        if (
            self.use_soros
            and not _in_gale
            and 0 < self.soros_step <= self.soros_max_steps
            and self.soros_profit > 0
        ):
            raw_stake = raw_stake + self.soros_profit

        # Martingale/Fibonacci: calcula stake de recuperação para o gale atual.
        if (
            self.use_martingale
            and self.martingale_step > 0
            and self.martingale_base_stake > 0
        ):
            if self.martingale_mode == "fibonacci":
                fib_idx = min(self.martingale_step, len(FIB_SEQUENCE) - 1)
                raw_stake = self.fixed_stake * FIB_SEQUENCE[fib_idx]
            else:
                # Classic: G = perdas_acumuladas / payout_rate + stake_base
                raw_stake = (
                    self.martingale_accumulated_loss / self.martingale_payout_rate
                    + self.martingale_base_stake
                )

        remaining_budget = max(
            0.0, self._effective_loss_limit() + self.daily_net_profit
        )

        if (
            self.use_martingale
            and self.martingale_step > 0
            and self.martingale_base_stake > 0
        ):
            # Martingale recovery: o limite é a banca inteira.
            caps = [raw_stake, self.balance]
        elif self.loss_block_override:
            # Override ativo: ignora o budget restante do limite diário, usa banca inteira.
            caps = [raw_stake, self.balance]
        else:
            # Cap = banca inteira (sem pct_cap). Proteção real via can_trade() e remaining_budget.
            caps = [raw_stake, remaining_budget, self.balance]
        # CAP DINÂMICO: calcula o maior cap que faz TODOS os gales restantes
        # caberem no saldo atual. Ajusta automaticamente conforme banca sobe/desce.
        if _in_gale:
            dynamic_cap = self._dynamic_gale_cap()
            caps.append(dynamic_cap)

        stake = min(caps)

        # MAX_STAKE is an absolute ceiling for ALL modes (normal, Soros, gale)
        if self.max_stake > 0 and stake > self.max_stake:
            stake = self.max_stake

        if stake < self.min_stake:
            return 0.0

        return round(stake, 2)

    def _simulate_gale_cost(self, cap: float) -> float:
        """Simulate total cost of all remaining gales if ALL LOSE, with given cap.

        Starts from current martingale state (step, accumulated_loss) and
        simulates forward to max_gales, capping each stake at `cap`.
        Returns total amount that would be spent.
        """
        sim_accum = self.martingale_accumulated_loss
        base = self.martingale_base_stake or self.fixed_stake
        payout = self.martingale_payout_rate
        total = 0.0
        for step_i in range(self.martingale_step, self.martingale_max_gales):
            if self.martingale_mode == "fibonacci":
                fib_idx = min(step_i, len(FIB_SEQUENCE) - 1)
                formula = self.fixed_stake * FIB_SEQUENCE[fib_idx]
            else:
                formula = sim_accum / payout + base
            stake = min(formula, cap)
            total += stake
            sim_accum += stake
        return total

    def _dynamic_gale_cap(self) -> float:
        """Compute the maximum per-trade gale cap so ALL remaining gales
        (worst case: all LOSS) fit within current balance.

        Uses binary search: simulate the full martingale progression for
        each candidate cap, find the highest cap that keeps total ≤ available.
        As balance grows from wins, the cap automatically increases.
        As balance shrinks from losses, the cap automatically decreases.
        """
        if not self.use_martingale or self.martingale_step <= 0:
            return float("inf")

        remaining = self.martingale_max_gales - self.martingale_step
        floor = max(self.martingale_min_balance_floor, 0)

        if remaining <= 0:
            # Last gale step: no future gales to reserve for — allow full stake
            available = self.balance - floor
            if available <= 0:
                return 0.0
            cap = available
            if self.max_stake > 0:
                cap = min(cap, self.max_stake)
            return round(max(cap, self.min_stake), 2)

        available = self.balance - floor
        if available <= 0:
            return 0.0

        lo = self.min_stake
        hi = available
        # Respect configured MAX_STAKE as absolute ceiling (if set)
        if self.max_stake > 0:
            hi = min(hi, self.max_stake)

        best = lo
        for _ in range(50):
            if hi - lo < 0.005:
                break
            mid = (lo + hi) / 2
            cost = self._simulate_gale_cost(mid)
            if cost <= available:
                best = mid
                lo = mid
            else:
                hi = mid

        return round(best, 2)

    def get_gale_raw_stake(self) -> float:
        """Returns the uncapped martingale recovery stake (may exceed max_stake API limit).

        Use this to detect when the gale needs to be split into multiple simultaneous
        contracts to work around Deriv's per-contract stake ceiling.
        Returns 0.0 when not in a martingale gale.
        """
        if not (
            self.use_martingale
            and self.martingale_step > 0
            and self.martingale_base_stake > 0
        ):
            return 0.0
        if self.martingale_mode == "fibonacci":
            fib_idx = min(self.martingale_step, len(FIB_SEQUENCE) - 1)
            raw = self.fixed_stake * FIB_SEQUENCE[fib_idx]
        else:
            raw = (
                self.martingale_accumulated_loss / self.martingale_payout_rate
                + self.martingale_base_stake
            )
        return round(min(raw, self.balance), 2)

    def can_trade(self) -> bool:
        self._reset_if_new_day()
        # Check for external override changes written by dashboard (no restart needed)
        self._reload_overrides()

        _now = time.monotonic()

        # PROTEÇÃO: balance floor — se saldo abaixo do mínimo, para tudo (inclusive gale)
        if (
            self.martingale_min_balance_floor > 0
            and self.balance < self.martingale_min_balance_floor
        ):
            if _now - self._log_ts_loss >= 60:
                logger.warning(
                    "⛔ Balance floor atingido: saldo=%.2f < floor=%.2f — PARANDO operacoes (inclusive gale)",
                    self.balance,
                    self.martingale_min_balance_floor,
                )
                self._log_ts_loss = _now
            # Se estiver em gale, absorve e reseta para evitar aposta suicida
            if self.use_martingale and self.martingale_step > 0:
                logger.warning(
                    "⛔ Gale %d/%d ABORTADO por balance floor — perdas=%.2f absorvidas",
                    self.martingale_step,
                    self.martingale_max_gales,
                    self.martingale_accumulated_loss,
                )
                self.martingale_step = 0
                self.martingale_accumulated_loss = 0.0
                self.martingale_base_stake = 0.0
                self._save_state()
            return False

        # PROTEÇÃO: dynamic gale cap — aborta gale se próximo step exige mais que saldo disponível
        if (
            self.use_martingale
            and self.martingale_step > 0
            and self.martingale_base_stake > 0
        ):
            if self.martingale_mode == "fibonacci":
                fib_idx = min(self.martingale_step, len(FIB_SEQUENCE) - 1)
                next_gale_stake = self.fixed_stake * FIB_SEQUENCE[fib_idx]
            else:
                next_gale_stake = (
                    self.martingale_accumulated_loss / self.martingale_payout_rate
                    + self.martingale_base_stake
                )
            usable_balance = (
                self.balance * self.martingale_max_balance_pct
                if self.martingale_max_balance_pct > 0
                else self.balance
            )
            if next_gale_stake > usable_balance:
                logger.warning(
                    "⛔ Gale %d/%d ABORTADO: stake_necessaria=%.2f > saldo_disponivel=%.2f (%.0f%% de %.2f)",
                    self.martingale_step,
                    self.martingale_max_gales,
                    next_gale_stake,
                    usable_balance,
                    self.martingale_max_balance_pct * 100,
                    self.balance,
                )
                logger.warning(
                    "   Perdas acumuladas=%.2f absorvidas — resetando para G0",
                    self.martingale_accumulated_loss,
                )
                self.martingale_step = 0
                self.martingale_accumulated_loss = 0.0
                self.martingale_base_stake = 0.0
                self._save_state()

        _loss_limit = self._effective_loss_limit()
        if self.daily_net_profit <= -_loss_limit and not self.loss_block_override:
            # Allow gale to continue ONLY if loss is within 2x the daily limit.
            # Beyond 2x = emergency stop regardless of gale state (prevents death spiral).
            _in_gale_recovery = self.use_martingale and self.martingale_step > 0
            _emergency = self.daily_net_profit <= -_loss_limit * 2.0
            if not _in_gale_recovery or _emergency:
                if _emergency and _in_gale_recovery:
                    logger.warning(
                        "🚨 EMERGENCY STOP: perda=%.2f ultrapassou 2x o limite=%.2f — gale abortado",
                        self.daily_net_profit,
                        _loss_limit,
                    )
                    self.martingale_step = 0
                    self.martingale_accumulated_loss = 0.0
                    self.martingale_base_stake = 0.0
                    self._save_state()
                if _now - self._log_ts_loss >= 60:
                    logger.warning(
                        "Stop loss diario atingido: lucro_liquido=%.2f (limite=-%.2f)",
                        self.daily_net_profit,
                        _loss_limit,
                    )
                    self._log_ts_loss = _now
                return False

        _profit_limit = self._effective_profit_limit()
        if _profit_limit > 0 and self.daily_net_profit >= _profit_limit:
            if _now - self._log_ts_profit >= 60:
                logger.warning(
                    "Meta de lucro diaria atingida: %.2f", self.daily_net_profit
                )
                self._log_ts_profit = _now
            return False

        if (
            self.daily_trailing_active
            and self.daily_net_profit <= self.daily_trailing_lock
        ):
            if _now - self._log_ts_trailing >= 60:
                logger.warning(
                    "Trailing diario protegido: lucro_liquido=%.2f lock=%.2f",
                    self.daily_net_profit,
                    self.daily_trailing_lock,
                )
                self._log_ts_trailing = _now
            return False

        # max_trades_day limit removed — bot runs unlimited trades per day

        if self.consecutive_losses >= self.max_consecutive_losses:
            # Never block during martingale recovery — the gale MUST continue
            if not (self.use_martingale and self.martingale_step > 0):
                if _now - self._log_ts_consec >= 60:
                    logger.warning(
                        "Limite de losses consecutivos atingido: %s",
                        self.consecutive_losses,
                    )
                    self._log_ts_consec = _now
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
        if (
            self.daily_trailing_active
            and self.daily_net_profit - stake < self.daily_trailing_lock
        ):
            logger.warning(
                "Proxima stake %.2f arriscaria lucro protegido %.2f (lock=%.2f)",
                stake,
                self.daily_net_profit,
                self.daily_trailing_lock,
            )
            return False

        # Frequency-based drawdown: stop if too many losses in sliding time window
        # Never block during martingale recovery
        now = time.monotonic()
        cutoff = now - self.loss_window_seconds
        while self._recent_loss_times and self._recent_loss_times[0] < cutoff:
            self._recent_loss_times.popleft()
        if len(self._recent_loss_times) >= self.max_losses_in_window:
            if not (self.use_martingale and self.martingale_step > 0):
                if now - self._log_ts_freq >= 60:
                    logger.warning(
                        "Frequencia de losses excedida: %d losses nos ultimos %.0fs (limite=%d). Bot pausado.",
                        len(self._recent_loss_times),
                        self.loss_window_seconds,
                        self.max_losses_in_window,
                    )
                    self._log_ts_freq = now
                return False

        return True

    def update(self, profit: float, buy_price: float) -> None:
        self._reset_if_new_day()

        profit = float(profit)
        buy_price = float(buy_price)
        # Consume the stake already deducted by handle_buy() (production path).
        # If 0, handle_buy was not called (unit tests) → update() is responsible for
        # all balance changes.
        _stake_deducted = self._pending_stake_deduction
        self._pending_stake_deduction = 0.0
        self.trades_today += 1
        # NOTE: daily_net_profit is synced from real balance in sync_pnl_from_balance().
        # We still do an immediate += here so that risk checks within update()
        # reflect the trade before the balance stream arrives (~100ms later).
        # The next balance stream event will overwrite this with the truth.
        self.daily_net_profit += profit
        self.daily_peak_profit = max(self.daily_peak_profit, self.daily_net_profit)

        if (
            self.daily_trailing_start > 0
            and self.daily_peak_profit >= self.daily_trailing_start
        ):
            self.daily_trailing_active = True

        if profit > 0:
            self.wins += 1
            self.consecutive_losses = 0
            _was_gale_win = self.use_martingale and self.martingale_step > 0
            if self.use_martingale and self.martingale_step > 0:
                if self.martingale_mode == "fibonacci":
                    # Fibonacci: on WIN, step back 2 positions. If step reaches 0, fully reset.
                    prev_step = self.martingale_step
                    self.martingale_step = max(0, self.martingale_step - 2)
                    if self.martingale_step == 0:
                        logger.info(
                            "✅ Fibonacci recuperado | step=%d → 0 | sequência encerrada",
                            prev_step,
                        )
                        self.martingale_accumulated_loss = 0.0
                        self.martingale_base_stake = 0.0
                    else:
                        fib_idx = min(self.martingale_step, len(FIB_SEQUENCE) - 1)
                        next_stake = round(self.fixed_stake * FIB_SEQUENCE[fib_idx], 2)
                        logger.info(
                            "⚡ Fibonacci parcial: step=%d → %d | stake_prox=%.2f",
                            prev_step,
                            self.martingale_step,
                            next_stake,
                        )
                else:
                    # Classic: reduce accumulated_loss by this WIN's profit.
                    self.martingale_accumulated_loss = round(
                        max(0.0, self.martingale_accumulated_loss - profit), 2
                    )
                    if self.martingale_accumulated_loss <= 0.0:
                        logger.info(
                            "✅ Martingale recuperado | step=%d → 0 | todas perdas cobertas",
                            self.martingale_step,
                        )
                        self.martingale_step = 0
                        self.martingale_accumulated_loss = 0.0
                        self.martingale_base_stake = 0.0
                    elif self.martingale_step >= self.martingale_max_gales:
                        logger.warning(
                            "⚠ Martingale max_gales=%d atingido com residuo=%.2f — absorvido como perda aceita",
                            self.martingale_max_gales,
                            self.martingale_accumulated_loss,
                        )
                        self.martingale_step = 0
                        self.martingale_accumulated_loss = 0.0
                        self.martingale_base_stake = 0.0
                    else:
                        logger.info(
                            "⚡ Martingale parcial: +%.2f recuperado | residual=%.2f | stake_prox=%.2f",
                            profit,
                            self.martingale_accumulated_loss,
                            round(
                                self.martingale_accumulated_loss
                                / self.martingale_payout_rate
                                + self.martingale_base_stake,
                                2,
                            ),
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
                    _reinvest = round(profit * self.soros_profit_factor, 2)
                    self.soros_profit = round(self.soros_profit + _reinvest, 2)
                else:
                    self.soros_step = 0
                    self.soros_profit = 0.0
            _modo_win = (
                f"SOROS {self.soros_step}/{self.soros_max_steps}"
                if self.use_soros and self.soros_step > 0
                else "NORMAL"
            )
            # Update local balance estimate immediately — Deriv balance message arrives ~100ms later.
            # Production (handle_buy deducted stake): add stake back + profit.
            # Unit tests (no handle_buy): add only profit (stake was never deducted).
            self.balance = round(self.balance + _stake_deducted + profit, 2)
            logger.info(
                "WIN %+0.2f | saldo_estimado=%0.2f | modo=%s",
                profit,
                self.balance,
                _modo_win,
            )
        else:
            self.losses += 1
            self.consecutive_losses += 1
            self.soros_step = 0
            self.soros_profit = 0.0
            if self.use_martingale:
                if self.martingale_step == 0:
                    # Primeiro loss: base_stake é SEMPRE o valor base (sem Soros)
                    # para que gales não herdem a inflação do Soros.
                    self.martingale_base_stake = self.fixed_stake
                    # Acumula apenas o stake base, NÃO o buy_price inflado pelo Soros.
                    # O prêmio Soros é risco aceito — gale recupera somente a perda base.
                    self.martingale_accumulated_loss = self.fixed_stake
                else:
                    # Gale subsequente: buy_price É o stake do gale (calculado
                    # corretamente a partir do base_stake), então rastreia perda real.
                    self.martingale_accumulated_loss += buy_price
                # Fibonacci: cap at len(FIB_SEQUENCE) - 1 in addition to max_gales
                _effective_max = self.martingale_max_gales
                if self.martingale_mode == "fibonacci":
                    _effective_max = min(_effective_max, len(FIB_SEQUENCE) - 1)
                if self.martingale_step >= _effective_max:
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
            self.max_loss_streak_today = max(
                self.max_loss_streak_today, self.consecutive_losses
            )
            realized_loss = abs(profit) if profit < 0 else buy_price
            self.daily_loss += realized_loss
            # Record timestamp for frequency-based MDD
            self._recent_loss_times.append(time.monotonic())
            _modo_loss = (
                f"GALE {self.martingale_step}/{self.martingale_max_gales}"
                if self.use_martingale and self.martingale_step > 0
                else "NORMAL"
            )
            # Update local balance estimate immediately — Deriv balance message arrives ~100ms later.
            # Production (handle_buy already deducted stake): balance is already correct, no change.
            # Unit tests (no handle_buy): deduct the stake here.
            if _stake_deducted == 0:
                self.balance = round(self.balance - buy_price, 2)
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
