from __future__ import annotations

import logging
from pathlib import Path

from sales_etl.config import EXPECTED_COLUMNS


class FileValidator:
    """Perform file-level validations before Bronze ingestion."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def validate_file_exists(self, file_path: str) -> None:
        """Validate that the source file exists."""

        self.logger.info(
            "Validating file existence. File=%s",
            file_path,
        )

        if not Path(file_path).exists():
            raise FileNotFoundError(
                f"Source file not found: {file_path}"
            )

    def validate_not_empty(self, file_path: str) -> None:
        """Validate that the source file is not empty."""

        self.logger.info(
            "Validating file is not empty. File=%s",
            file_path,
        )

        if Path(file_path).stat().st_size == 0:
            raise ValueError(
                f"Input file is empty: {file_path}"
            )

    def validate_header(self, columns: list[str]) -> None:
        """Validate the source file header."""

        self.logger.info("Validating file header.")

        if tuple(columns) != EXPECTED_COLUMNS:
            raise ValueError(
                "Header validation failed. "
                f"Expected={EXPECTED_COLUMNS}, "
                f"Actual={tuple(columns)}"
            )

        self.logger.info(
            "Header validation completed successfully."
        )
