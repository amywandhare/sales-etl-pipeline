import logging
from pathlib import Path

from sales_etl.utils.logging_config import (
    configure_logging,
)


def test_configure_logging_creates_log_file(
    tmp_path,
):
    logger = configure_logging(
        tmp_path
    )

    logger.info(
        "test message"
    )

    log_file = (
        tmp_path
        / "pipeline.log"
    )

    assert log_file.exists()


def test_configure_logging_returns_logger(
    tmp_path,
):
    logger = configure_logging(
        tmp_path
    )

    assert isinstance(
        logger,
        logging.Logger,
    )

    assert (
        logger.name
        == "sales_etl"
    )


def test_configure_logging_reuses_existing_logger(
    tmp_path,
):
    logger1 = configure_logging(
        tmp_path
    )

    handler_count = len(
        logger1.handlers
    )

    logger2 = configure_logging(
        tmp_path
    )

    assert logger1 is logger2

    assert (
        len(logger2.handlers)
        == handler_count
    )


def test_configure_logging_switches_log_directory(
    tmp_path,
):
    log_dir1 = (
        tmp_path / "logs1"
    )

    log_dir2 = (
        tmp_path / "logs2"
    )

    logger = configure_logging(
        log_dir1
    )

    logger = configure_logging(
        log_dir2
    )

    file_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(
            handler,
            logging.FileHandler,
        )
    ]

    assert len(
        file_handlers
    ) == 1

    assert Path(
        file_handlers[0].baseFilename
    ).parent == log_dir2.resolve()


def test_configure_logging_uses_default_directory(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(
        tmp_path
    )

    logger = configure_logging()

    logger.info(
        "default log message"
    )

    assert (
        tmp_path
        / "logs"
        / "pipeline.log"
    ).exists()