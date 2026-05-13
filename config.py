import os
from dataclasses import dataclass

from dotenv import load_dotenv

from strategy import StrategyConfig


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value == "" else int(value)


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None or value == "" else float(value)


def _hours_env(name: str) -> tuple[int, ...]:
    value = os.getenv(name, "").strip()
    if not value:
        return ()

    hours: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = [int(item.strip()) for item in part.split("-", 1)]
            if start > end:
                raise ValueError(f"{name} tem intervalo invalido: {part}")
            hours.update(range(start, end + 1))
        else:
            hours.add(int(part))

    invalid = [hour for hour in hours if hour < 0 or hour > 23]
    if invalid:
        raise ValueError(f"{name} contem hora UTC invalida: {invalid}")
    return tuple(sorted(hours))


@dataclass(frozen=True)
class BotConfig:
    bot_name: str
    token: str
    app_id: str
    account_mode: str
    symbol: str
    currency: str
    stake: float
    duration: int
    duration_unit: str
    granularity: int
    candle_count: int
    min_signal_score: int
    use_trend_filter: bool
    trend_ema_window: int
    use_atr_filter: bool
    atr_window: int
    min_atr_percent: float
    rsi_extreme_weight: int
    rsi_soft_weight: int
    macd_cross_weight: int
    bollinger_touch_weight: int
    ema_cross_weight: int
    blocked_utc_hours: tuple[int, ...]
    max_loss_per_day: float
    max_profit_per_day: float
    max_trades_per_day: int
    daily_trailing_start: float
    daily_trailing_lock: float
    max_stake_percent: float
    max_consecutive_losses: int
    min_stake: float
    max_stake: float
    use_soros: bool
    soros_max_steps: int
    soros_profit_factor: float
    cooldown_candles: int
    journal_dir: str
    dry_run: bool
    allow_real_trading: bool
    reconnect_delay_seconds: int

    @property
    def ws_url(self) -> str:
        return f"wss://ws.derivws.com/websockets/v3?app_id={self.app_id}"

    @property
    def strategy_config(self) -> StrategyConfig:
        return StrategyConfig(
            min_score=self.min_signal_score,
            use_trend_filter=self.use_trend_filter,
            trend_ema_window=self.trend_ema_window,
            use_atr_filter=self.use_atr_filter,
            atr_window=self.atr_window,
            min_atr_percent=self.min_atr_percent,
            rsi_extreme_weight=self.rsi_extreme_weight,
            rsi_soft_weight=self.rsi_soft_weight,
            macd_cross_weight=self.macd_cross_weight,
            bollinger_touch_weight=self.bollinger_touch_weight,
            ema_cross_weight=self.ema_cross_weight,
        )


def load_config() -> BotConfig:
    load_dotenv()

    token = os.getenv("DERIV_TOKEN", "").strip()
    if not token:
        raise ValueError("DERIV_TOKEN nao foi definido. Crie um .env a partir de .env.example.")

    app_id = os.getenv("DERIV_APP_ID", "1089").strip()
    if not app_id:
        raise ValueError("DERIV_APP_ID nao foi definido.")

    config = BotConfig(
        bot_name=os.getenv("BOT_NAME", "Pegasus").strip() or "Pegasus",
        token=token,
        app_id=app_id,
        account_mode=os.getenv("ACCOUNT_MODE", "demo").strip().lower(),
        symbol=os.getenv("SYMBOL", "R_100").strip(),
        currency=os.getenv("CURRENCY", "USD").strip().upper(),
        stake=_float_env("STAKE", 1.0),
        duration=_int_env("DURATION", 5),
        duration_unit=os.getenv("DURATION_UNIT", "m").strip(),
        granularity=_int_env("GRANULARITY", 60),
        candle_count=_int_env("CANDLE_COUNT", 260),
        min_signal_score=_int_env("MIN_SIGNAL_SCORE", 5),
        use_trend_filter=_bool_env("USE_TREND_FILTER", True),
        trend_ema_window=_int_env("TREND_EMA_WINDOW", 200),
        use_atr_filter=_bool_env("USE_ATR_FILTER", True),
        atr_window=_int_env("ATR_WINDOW", 14),
        min_atr_percent=_float_env("MIN_ATR_PERCENT", 0.05),
        rsi_extreme_weight=_int_env("RSI_EXTREME_WEIGHT", 3),
        rsi_soft_weight=_int_env("RSI_SOFT_WEIGHT", 1),
        macd_cross_weight=_int_env("MACD_CROSS_WEIGHT", 3),
        bollinger_touch_weight=_int_env("BOLLINGER_TOUCH_WEIGHT", 2),
        ema_cross_weight=_int_env("EMA_CROSS_WEIGHT", 2),
        blocked_utc_hours=_hours_env("BLOCKED_UTC_HOURS"),
        max_loss_per_day=_float_env("MAX_LOSS_PER_DAY", 20.0),
        max_profit_per_day=_float_env("MAX_PROFIT_PER_DAY", 0.0),
        max_trades_per_day=_int_env("MAX_TRADES_PER_DAY", 50),
        daily_trailing_start=_float_env("DAILY_TRAILING_START", 0.0),
        daily_trailing_lock=_float_env("DAILY_TRAILING_LOCK", 0.0),
        max_stake_percent=_float_env("MAX_STAKE_PERCENT", 0.02),
        max_consecutive_losses=_int_env("MAX_CONSECUTIVE_LOSSES", 7),
        min_stake=_float_env("MIN_STAKE", 0.35),
        max_stake=_float_env("MAX_STAKE", 100.0),
        use_soros=_bool_env("USE_SOROS", False),
        soros_max_steps=_int_env("SOROS_MAX_STEPS", 1),
        soros_profit_factor=_float_env("SOROS_PROFIT_FACTOR", 1.0),
        cooldown_candles=_int_env("COOLDOWN_CANDLES", 1),
        journal_dir=os.getenv("JOURNAL_DIR", "logs").strip() or "logs",
        dry_run=_bool_env("DRY_RUN", True),
        allow_real_trading=_bool_env("ALLOW_REAL_TRADING", False),
        reconnect_delay_seconds=_int_env("RECONNECT_DELAY_SECONDS", 10),
    )

    if config.stake <= 0:
        raise ValueError("STAKE precisa ser maior que zero.")
    if config.duration <= 0:
        raise ValueError("DURATION precisa ser maior que zero.")
    if config.granularity <= 0:
        raise ValueError("GRANULARITY precisa ser maior que zero.")
    if config.candle_count < config.strategy_config.minimum_candles:
        raise ValueError(
            f"CANDLE_COUNT deve ser pelo menos {config.strategy_config.minimum_candles} "
            "para os filtros configurados."
        )
    if config.account_mode not in {"demo", "real", "any"}:
        raise ValueError("ACCOUNT_MODE deve ser demo, real ou any.")
    if config.trend_ema_window <= 0:
        raise ValueError("TREND_EMA_WINDOW precisa ser maior que zero.")
    if config.atr_window <= 0:
        raise ValueError("ATR_WINDOW precisa ser maior que zero.")
    if config.min_atr_percent < 0:
        raise ValueError("MIN_ATR_PERCENT nao pode ser negativo.")
    for name, value in {
        "RSI_EXTREME_WEIGHT": config.rsi_extreme_weight,
        "RSI_SOFT_WEIGHT": config.rsi_soft_weight,
        "MACD_CROSS_WEIGHT": config.macd_cross_weight,
        "BOLLINGER_TOUCH_WEIGHT": config.bollinger_touch_weight,
        "EMA_CROSS_WEIGHT": config.ema_cross_weight,
    }.items():
        if value < 0:
            raise ValueError(f"{name} nao pode ser negativo.")
    if config.max_loss_per_day <= 0:
        raise ValueError("MAX_LOSS_PER_DAY precisa ser maior que zero.")
    if config.max_profit_per_day < 0:
        raise ValueError("MAX_PROFIT_PER_DAY nao pode ser negativo.")
    if config.max_trades_per_day <= 0:
        raise ValueError("MAX_TRADES_PER_DAY precisa ser maior que zero.")
    if config.daily_trailing_start < 0 or config.daily_trailing_lock < 0:
        raise ValueError("DAILY_TRAILING_START/LOCK nao podem ser negativos.")
    if config.daily_trailing_lock > config.daily_trailing_start and config.daily_trailing_start > 0:
        raise ValueError("DAILY_TRAILING_LOCK nao pode ser maior que DAILY_TRAILING_START.")
    if not 0 < config.max_stake_percent <= 1:
        raise ValueError("MAX_STAKE_PERCENT deve estar entre 0 e 1.")
    if config.min_stake <= 0 or config.max_stake < config.min_stake:
        raise ValueError("MIN_STAKE/MAX_STAKE invalidos.")
    if config.soros_max_steps < 0:
        raise ValueError("SOROS_MAX_STEPS nao pode ser negativo.")
    if not 0 <= config.soros_profit_factor <= 1:
        raise ValueError("SOROS_PROFIT_FACTOR deve estar entre 0 e 1.")
    if config.cooldown_candles < 0:
        raise ValueError("COOLDOWN_CANDLES nao pode ser negativo.")

    return config
