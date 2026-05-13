import os
from dataclasses import dataclass

from dotenv import load_dotenv


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
    max_loss_per_day: float
    max_profit_per_day: float
    max_trades_per_day: int
    max_stake_percent: float
    max_consecutive_losses: int
    min_stake: float
    max_stake: float
    cooldown_candles: int
    journal_dir: str
    dry_run: bool
    allow_real_trading: bool
    reconnect_delay_seconds: int

    @property
    def ws_url(self) -> str:
        return f"wss://ws.derivws.com/websockets/v3?app_id={self.app_id}"


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
        candle_count=_int_env("CANDLE_COUNT", 100),
        min_signal_score=_int_env("MIN_SIGNAL_SCORE", 5),
        max_loss_per_day=_float_env("MAX_LOSS_PER_DAY", 20.0),
        max_profit_per_day=_float_env("MAX_PROFIT_PER_DAY", 0.0),
        max_trades_per_day=_int_env("MAX_TRADES_PER_DAY", 50),
        max_stake_percent=_float_env("MAX_STAKE_PERCENT", 0.02),
        max_consecutive_losses=_int_env("MAX_CONSECUTIVE_LOSSES", 7),
        min_stake=_float_env("MIN_STAKE", 0.35),
        max_stake=_float_env("MAX_STAKE", 100.0),
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
    if config.candle_count < 40:
        raise ValueError("CANDLE_COUNT deve ser pelo menos 40 para RSI/MACD/Bollinger.")
    if config.account_mode not in {"demo", "real", "any"}:
        raise ValueError("ACCOUNT_MODE deve ser demo, real ou any.")
    if config.max_loss_per_day <= 0:
        raise ValueError("MAX_LOSS_PER_DAY precisa ser maior que zero.")
    if config.max_profit_per_day < 0:
        raise ValueError("MAX_PROFIT_PER_DAY nao pode ser negativo.")
    if config.max_trades_per_day <= 0:
        raise ValueError("MAX_TRADES_PER_DAY precisa ser maior que zero.")
    if not 0 < config.max_stake_percent <= 1:
        raise ValueError("MAX_STAKE_PERCENT deve estar entre 0 e 1.")
    if config.min_stake <= 0 or config.max_stake < config.min_stake:
        raise ValueError("MIN_STAKE/MAX_STAKE invalidos.")
    if config.cooldown_candles < 0:
        raise ValueError("COOLDOWN_CANDLES nao pode ser negativo.")

    return config
