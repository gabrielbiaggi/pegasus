import os
from dataclasses import dataclass

from dotenv import load_dotenv

from strategy import AccumulatorStrategyConfig


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
    contract_mode: str
    symbol: str
    currency: str
    stake: float
    blocked_utc_hours: tuple[int, ...]
    block_weekends: bool
    max_loss_per_day: float
    max_loss_day_pct: float  # 0.0 = use max_loss_per_day; >0 = % of balance (overrides fixed)
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
    use_dynamic_stake: bool
    dynamic_stake_base_pct: float
    journal_dir: str
    dry_run: bool
    allow_real_trading: bool
    reconnect_delay_seconds: int
    tick_count: int
    accumulator_growth_rate: float
    accumulator_take_profit_percent: float
    accumulator_max_hold_ticks: int
    accumulator_shadow_barrier_atr_multiplier: float
    accumulator_shadow_barrier_min_percent: float
    accumulator_shadow_barrier_max_percent: float
    accumulator_shadow_proposal_enabled: bool
    accumulator_shadow_proposal_throttle_seconds: int
    accumulator_shadow_proposal_min_score: int
    accumulator_cooldown_ticks: int
    accumulator_use_limit_order: bool
    accumulator_min_score: int
    accumulator_bb_window: int
    accumulator_bb_std_dev: float
    accumulator_max_bb_width_percent: float
    accumulator_atr_window: int
    accumulator_max_tick_atr_percent: float
    accumulator_recent_window: int
    accumulator_max_recent_move_percent: float
    accumulator_hawkes_alpha: float
    accumulator_hawkes_beta: float
    accumulator_hawkes_jump_atr_multiplier: float
    accumulator_max_hawkes_intensity: float
    accumulator_imbalance_window: int
    accumulator_max_abs_tick_imbalance: int
    accumulator_hurst_window: int
    accumulator_max_hurst_exponent: float
    accumulator_derivative_window: int
    accumulator_max_velocity_zscore: float
    accumulator_max_acceleration_zscore: float
    accumulator_integral_window: int
    accumulator_max_pmi_distance_percent: float
    accumulator_markov_window: int
    accumulator_max_markov_continuation_prob: float
    accumulator_shannon_entropy_window: int
    accumulator_min_shannon_entropy: float
    accumulator_kalman_q: float
    accumulator_kalman_r: float
    accumulator_max_kalman_residual_zscore: float
    accumulator_use_ensemble: bool
    accumulator_ensemble_min_prob: float

    @property
    def ws_url(self) -> str:
        return f"wss://ws.derivws.com/websockets/v3?app_id={self.app_id}"

    @property
    def accumulator_strategy_config(self) -> AccumulatorStrategyConfig:
        return AccumulatorStrategyConfig(
            min_score=self.accumulator_min_score,
            bb_window=self.accumulator_bb_window,
            bb_std_dev=self.accumulator_bb_std_dev,
            max_bb_width_percent=self.accumulator_max_bb_width_percent,
            atr_window=self.accumulator_atr_window,
            max_tick_atr_percent=self.accumulator_max_tick_atr_percent,
            recent_window=self.accumulator_recent_window,
            max_recent_move_percent=self.accumulator_max_recent_move_percent,
            hawkes_alpha=self.accumulator_hawkes_alpha,
            hawkes_beta=self.accumulator_hawkes_beta,
            hawkes_jump_atr_multiplier=self.accumulator_hawkes_jump_atr_multiplier,
            max_hawkes_intensity=self.accumulator_max_hawkes_intensity,
            imbalance_window=self.accumulator_imbalance_window,
            max_abs_tick_imbalance=self.accumulator_max_abs_tick_imbalance,
            hurst_window=self.accumulator_hurst_window,
            max_hurst_exponent=self.accumulator_max_hurst_exponent,
            derivative_window=self.accumulator_derivative_window,
            max_velocity_zscore=self.accumulator_max_velocity_zscore,
            max_acceleration_zscore=self.accumulator_max_acceleration_zscore,
            integral_window=self.accumulator_integral_window,
            max_pmi_distance_percent=self.accumulator_max_pmi_distance_percent,
            markov_window=self.accumulator_markov_window,
            max_markov_continuation_prob=self.accumulator_max_markov_continuation_prob,
            shannon_entropy_window=self.accumulator_shannon_entropy_window,
            min_shannon_entropy=self.accumulator_min_shannon_entropy,
            kalman_q=self.accumulator_kalman_q,
            kalman_r=self.accumulator_kalman_r,
            max_kalman_residual_zscore=self.accumulator_max_kalman_residual_zscore,
            use_ensemble=self.accumulator_use_ensemble,
            ensemble_min_prob=self.accumulator_ensemble_min_prob,
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
        contract_mode=os.getenv("CONTRACT_MODE", "accumulator").strip().lower(),
        symbol=os.getenv("SYMBOL", "1HZ100V").strip(),
        currency=os.getenv("CURRENCY", "USD").strip().upper(),
        stake=_float_env("STAKE", 1.0),
        blocked_utc_hours=_hours_env("BLOCKED_UTC_HOURS"),
        block_weekends=_bool_env("BLOCK_WEEKENDS", True),
        max_loss_per_day=_float_env("MAX_LOSS_PER_DAY", 20.0),
        max_loss_day_pct=_float_env("MAX_LOSS_DAY_PCT", 0.0),
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
        use_dynamic_stake=_bool_env("DYNAMIC_STAKE", True),
        dynamic_stake_base_pct=_float_env("DYNAMIC_STAKE_BASE_PCT", 0.02),
        journal_dir=os.getenv("JOURNAL_DIR", "logs").strip() or "logs",
        dry_run=_bool_env("DRY_RUN", True),
        allow_real_trading=_bool_env("ALLOW_REAL_TRADING", False),
        reconnect_delay_seconds=_int_env("RECONNECT_DELAY_SECONDS", 10),
        tick_count=_int_env("TICK_COUNT", 300),
        accumulator_growth_rate=_float_env("ACCUMULATOR_GROWTH_RATE", 0.03),
        accumulator_take_profit_percent=_float_env("ACCUMULATOR_TAKE_PROFIT_PERCENT", 3.0),
        accumulator_max_hold_ticks=_int_env("ACCUMULATOR_MAX_HOLD_TICKS", 8),
        accumulator_shadow_barrier_atr_multiplier=_float_env("ACCUMULATOR_SHADOW_BARRIER_ATR_MULTIPLIER", 5.0),
        accumulator_shadow_barrier_min_percent=_float_env("ACCUMULATOR_SHADOW_BARRIER_MIN_PERCENT", 0.03),
        accumulator_shadow_barrier_max_percent=_float_env("ACCUMULATOR_SHADOW_BARRIER_MAX_PERCENT", 0.10),
        accumulator_shadow_proposal_enabled=_bool_env("ACCUMULATOR_SHADOW_PROPOSAL_ENABLED", True),
        accumulator_shadow_proposal_throttle_seconds=_int_env("ACCUMULATOR_SHADOW_PROPOSAL_THROTTLE_SECONDS", 5),
        accumulator_shadow_proposal_min_score=_int_env("ACCUMULATOR_SHADOW_PROPOSAL_MIN_SCORE", 4),
        accumulator_cooldown_ticks=_int_env("ACCUMULATOR_COOLDOWN_TICKS", 3),
        accumulator_use_limit_order=_bool_env("ACCUMULATOR_USE_LIMIT_ORDER", False),
        accumulator_min_score=_int_env("ACCUMULATOR_MIN_SCORE", 7),
        accumulator_bb_window=_int_env("ACCUMULATOR_BB_WINDOW", 20),
        accumulator_bb_std_dev=_float_env("ACCUMULATOR_BB_STD_DEV", 2.0),
        accumulator_max_bb_width_percent=_float_env("ACCUMULATOR_MAX_BB_WIDTH_PERCENT", 0.08),
        accumulator_atr_window=_int_env("ACCUMULATOR_ATR_WINDOW", 20),
        accumulator_max_tick_atr_percent=_float_env("ACCUMULATOR_MAX_TICK_ATR_PERCENT", 0.015),
        accumulator_recent_window=_int_env("ACCUMULATOR_RECENT_WINDOW", 5),
        accumulator_max_recent_move_percent=_float_env("ACCUMULATOR_MAX_RECENT_MOVE_PERCENT", 0.05),
        accumulator_hawkes_alpha=_float_env("ACCUMULATOR_HAWKES_ALPHA", 1.0),
        accumulator_hawkes_beta=_float_env("ACCUMULATOR_HAWKES_BETA", 0.85),
        accumulator_hawkes_jump_atr_multiplier=_float_env("ACCUMULATOR_HAWKES_JUMP_ATR_MULTIPLIER", 1.5),
        accumulator_max_hawkes_intensity=_float_env("ACCUMULATOR_MAX_HAWKES_INTENSITY", 0.2),
        accumulator_imbalance_window=_int_env("ACCUMULATOR_IMBALANCE_WINDOW", 10),
        accumulator_max_abs_tick_imbalance=_int_env("ACCUMULATOR_MAX_ABS_TICK_IMBALANCE", 2),
        accumulator_hurst_window=_int_env("ACCUMULATOR_HURST_WINDOW", 30),
        accumulator_max_hurst_exponent=_float_env("ACCUMULATOR_MAX_HURST_EXPONENT", 0.45),
        accumulator_derivative_window=_int_env("ACCUMULATOR_DERIVATIVE_WINDOW", 20),
        accumulator_max_velocity_zscore=_float_env("ACCUMULATOR_MAX_VELOCITY_ZSCORE", 2.0),
        accumulator_max_acceleration_zscore=_float_env("ACCUMULATOR_MAX_ACCELERATION_ZSCORE", 2.0),
        accumulator_integral_window=_int_env("ACCUMULATOR_INTEGRAL_WINDOW", 20),
        accumulator_max_pmi_distance_percent=_float_env("ACCUMULATOR_MAX_PMI_DISTANCE_PERCENT", 0.005),
        accumulator_markov_window=_int_env("ACCUMULATOR_MARKOV_WINDOW", 50),
        accumulator_max_markov_continuation_prob=_float_env("ACCUMULATOR_MAX_MARKOV_CONTINUATION_PROB", 0.45),
        accumulator_shannon_entropy_window=_int_env("ACCUMULATOR_SHANNON_ENTROPY_WINDOW", 30),
        accumulator_min_shannon_entropy=_float_env("ACCUMULATOR_MIN_SHANNON_ENTROPY", 0.80),
        accumulator_kalman_q=_float_env("ACCUMULATOR_KALMAN_Q", 1e-5),
        accumulator_kalman_r=_float_env("ACCUMULATOR_KALMAN_R", 1e-2),
        accumulator_max_kalman_residual_zscore=_float_env("ACCUMULATOR_MAX_KALMAN_RESIDUAL_ZSCORE", 2.0),
        accumulator_use_ensemble=_bool_env("USE_ENSEMBLE", False),
        accumulator_ensemble_min_prob=_float_env("ENSEMBLE_MIN_PROB", 0.294),
    )

    if config.stake <= 0:
        raise ValueError("STAKE precisa ser maior que zero.")
    if config.account_mode not in {"demo", "real", "any"}:
        raise ValueError("ACCOUNT_MODE deve ser demo, real ou any.")
    if config.contract_mode != "accumulator":
        raise ValueError("CONTRACT_MODE deve ser accumulator. Pegasus agora opera somente Accumulators por ticks.")
    if config.max_loss_day_pct > 0:
        if not 0 < config.max_loss_day_pct <= 1:
            raise ValueError("MAX_LOSS_DAY_PCT deve estar entre 0 e 1 (ex: 0.10 = 10%).")
    elif config.max_loss_per_day <= 0:
        raise ValueError("MAX_LOSS_PER_DAY precisa ser maior que zero (ou defina MAX_LOSS_DAY_PCT).")
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
    if config.min_stake <= 0:
        raise ValueError("MIN_STAKE deve ser maior que zero.")
    if config.max_stake > 0 and config.max_stake < config.min_stake:
        raise ValueError("MAX_STAKE nao pode ser menor que MIN_STAKE (use 0 para desativar o cap absoluto).")
    if config.soros_max_steps < 0:
        raise ValueError("SOROS_MAX_STEPS nao pode ser negativo.")
    if not 0 <= config.soros_profit_factor <= 1:
        raise ValueError("SOROS_PROFIT_FACTOR deve estar entre 0 e 1.")
    if config.tick_count < config.accumulator_strategy_config.minimum_ticks:
        raise ValueError(
            f"TICK_COUNT deve ser pelo menos {config.accumulator_strategy_config.minimum_ticks} "
            "para Accumulators."
        )
    if config.accumulator_growth_rate not in {0.01, 0.02, 0.03, 0.04, 0.05}:
        raise ValueError("ACCUMULATOR_GROWTH_RATE deve ser 0.01, 0.02, 0.03, 0.04 ou 0.05.")
    if config.accumulator_take_profit_percent <= 0:
        raise ValueError("ACCUMULATOR_TAKE_PROFIT_PERCENT precisa ser maior que zero.")
    if config.accumulator_max_hold_ticks <= 0:
        raise ValueError("ACCUMULATOR_MAX_HOLD_TICKS precisa ser maior que zero.")
    if config.accumulator_shadow_barrier_atr_multiplier <= 0:
        raise ValueError("ACCUMULATOR_SHADOW_BARRIER_ATR_MULTIPLIER precisa ser maior que zero.")
    if config.accumulator_shadow_barrier_min_percent <= 0:
        raise ValueError("ACCUMULATOR_SHADOW_BARRIER_MIN_PERCENT precisa ser maior que zero.")
    if config.accumulator_shadow_barrier_max_percent < config.accumulator_shadow_barrier_min_percent:
        raise ValueError(
            "ACCUMULATOR_SHADOW_BARRIER_MAX_PERCENT precisa ser >= ACCUMULATOR_SHADOW_BARRIER_MIN_PERCENT."
        )
    if config.accumulator_shadow_proposal_throttle_seconds < 1:
        raise ValueError("ACCUMULATOR_SHADOW_PROPOSAL_THROTTLE_SECONDS precisa ser >= 1.")
    if config.accumulator_shadow_proposal_min_score < 0:
        raise ValueError("ACCUMULATOR_SHADOW_PROPOSAL_MIN_SCORE nao pode ser negativo.")
    if config.accumulator_cooldown_ticks < 0:
        raise ValueError("ACCUMULATOR_COOLDOWN_TICKS nao pode ser negativo.")
    if config.accumulator_min_score <= 0:
        raise ValueError("ACCUMULATOR_MIN_SCORE precisa ser maior que zero.")
    if config.accumulator_bb_window <= 1 or config.accumulator_atr_window <= 1:
        raise ValueError("Janelas do accumulator precisam ser maiores que 1.")
    if config.accumulator_recent_window <= 0:
        raise ValueError("ACCUMULATOR_RECENT_WINDOW precisa ser maior que zero.")
    if config.accumulator_imbalance_window <= 1:
        raise ValueError("ACCUMULATOR_IMBALANCE_WINDOW precisa ser maior que 1.")
    if config.accumulator_hurst_window <= 3:
        raise ValueError("ACCUMULATOR_HURST_WINDOW precisa ser maior que 3.")
    if config.accumulator_derivative_window <= 1:
        raise ValueError("ACCUMULATOR_DERIVATIVE_WINDOW precisa ser maior que 1.")
    if config.accumulator_integral_window <= 1:
        raise ValueError("ACCUMULATOR_INTEGRAL_WINDOW precisa ser maior que 1.")
    if config.accumulator_markov_window <= 2:
        raise ValueError("ACCUMULATOR_MARKOV_WINDOW precisa ser maior que 2.")
    if config.accumulator_shannon_entropy_window <= 2:
        raise ValueError("ACCUMULATOR_SHANNON_ENTROPY_WINDOW precisa ser maior que 2.")
    if config.accumulator_bb_std_dev <= 0:
        raise ValueError("ACCUMULATOR_BB_STD_DEV precisa ser maior que zero.")
    if config.accumulator_max_bb_width_percent < 0 or config.accumulator_max_tick_atr_percent < 0:
        raise ValueError("Filtros percentuais do accumulator nao podem ser negativos.")
    if config.accumulator_max_recent_move_percent < 0:
        raise ValueError("ACCUMULATOR_MAX_RECENT_MOVE_PERCENT nao pode ser negativo.")
    if config.accumulator_hawkes_alpha < 0 or config.accumulator_hawkes_beta < 0:
        raise ValueError("Parametros Hawkes nao podem ser negativos.")
    if config.accumulator_hawkes_jump_atr_multiplier <= 0:
        raise ValueError("ACCUMULATOR_HAWKES_JUMP_ATR_MULTIPLIER precisa ser maior que zero.")
    if config.accumulator_max_hawkes_intensity < 0:
        raise ValueError("ACCUMULATOR_MAX_HAWKES_INTENSITY nao pode ser negativo.")
    if config.accumulator_max_abs_tick_imbalance < 0:
        raise ValueError("ACCUMULATOR_MAX_ABS_TICK_IMBALANCE nao pode ser negativo.")
    if config.accumulator_max_hurst_exponent <= 0:
        raise ValueError("ACCUMULATOR_MAX_HURST_EXPONENT precisa ser maior que zero.")
    if config.accumulator_max_velocity_zscore < 0 or config.accumulator_max_acceleration_zscore < 0:
        raise ValueError("Limites de z-score das derivadas nao podem ser negativos.")
    if config.accumulator_max_pmi_distance_percent < 0:
        raise ValueError("ACCUMULATOR_MAX_PMI_DISTANCE_PERCENT nao pode ser negativo.")
    if not 0 <= config.accumulator_max_markov_continuation_prob <= 1:
        raise ValueError("ACCUMULATOR_MAX_MARKOV_CONTINUATION_PROB deve estar entre 0 e 1.")
    if not 0 <= config.accumulator_min_shannon_entropy <= 1:
        raise ValueError("ACCUMULATOR_MIN_SHANNON_ENTROPY deve estar entre 0 e 1.")
    if config.accumulator_kalman_q <= 0 or config.accumulator_kalman_r <= 0:
        raise ValueError("ACCUMULATOR_KALMAN_Q/R precisam ser maiores que zero.")
    if config.accumulator_max_kalman_residual_zscore < 0:
        raise ValueError("ACCUMULATOR_MAX_KALMAN_RESIDUAL_ZSCORE nao pode ser negativo.")

    return config
