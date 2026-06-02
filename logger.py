import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger() -> logging.Logger:
    is_optimizer = os.getenv("PEGASUS_OPTIMIZER_RUN", "false").lower() == "true"
    logger = logging.getLogger(os.getenv("BOT_NAME", "Pegasus"))
    if is_optimizer:
        logger.setLevel(logging.ERROR)
        logger.propagate = False
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    os.makedirs("logs", exist_ok=True)

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    import sys
    main_script = sys.argv[0] if sys.argv else ""
    is_live_bot = (
        "bot.py" in main_script
        and not any(x in main_script for x in ["backtest", "optimize", "sweep", "analyze", "test"])
    ) or os.getenv("PEGASUS_LIVE_BOT", "false").lower() == "true"

    log_filename = "logs/trades.log" if is_live_bot else "logs/backtest.log"
    backup_count = 5 if is_live_bot else 1

    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=5 * 1024 * 1024,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


logger = setup_logger()
