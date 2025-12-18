import io
import logging
import sys
from typing import Any, Dict, Optional


LOG_LEVEL_MAP = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}


def _level(log_levels: Dict[str, int], value: Optional[str], default: str) -> int:
    return log_levels.get((value or default).lower(), log_levels[default])


def _utf8_stream(stream):
    """
    Wrap a text stream to ensure UTF-8 output even when the default console encoding
    cannot represent certain characters (e.g. emoji on Windows cp1252 consoles).
    """
    if stream and getattr(stream, "encoding", "").lower() == "utf-8":
        return stream
    try:
        buffer = getattr(stream, "buffer", None)
        if buffer is not None:
            return io.TextIOWrapper(buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass
    return stream


def configure_logging(logging_config: Dict[str, Any]) -> logging.Logger:
    """
    Configure root logger for console/file outputs.
    Re-entrant safe: subsequent calls return the already configured logger.
    """
    logger = logging.getLogger('charybdisk')
    logger.setLevel(logging.DEBUG)

    if getattr(logger, '_charybdisk_configured', False):
        return logger

    console_log_level = _level(LOG_LEVEL_MAP, logging_config.get('console_log_level'), 'warning')
    file_log_level = _level(LOG_LEVEL_MAP, logging_config.get('file_log_level'), 'info')

    console_stream = _utf8_stream(sys.stdout)
    console_handler = logging.StreamHandler(console_stream)
    console_handler.setLevel(console_log_level)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

    log_file = logging_config.get('log_file')
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(file_log_level)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

    logger._charybdisk_configured = True  # type: ignore[attr-defined]
    return logger
