import logging
from pathlib import Path


def configure_logging(log_dir: str | Path | None = None) -> logging.Logger:
    """Configure console and file logging for one pipeline run."""
    log_root = Path(log_dir) if log_dir else Path("logs")
    log_root.mkdir(parents=True, exist_ok=True)
    log_path = (log_root / "pipeline.log").resolve()

    logger = logging.getLogger("sales_etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    current_file_handlers = [
        handler for handler in logger.handlers if isinstance(handler, logging.FileHandler)
    ]
    if current_file_handlers and any(
        Path(handler.baseFilename).resolve() != log_path for handler in current_file_handlers
    ):
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger