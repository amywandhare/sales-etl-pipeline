import logging

import pytest

from sales_etl.config import EXPECTED_COLUMNS
from sales_etl.validations.file_validator import (
    FileValidator,
)


@pytest.fixture
def validator():
    return FileValidator(
        logger=logging.getLogger(
            "test_validator"
        )
    )


def test_validate_file_exists_success(
    validator,
    tmp_path,
):
    file_path = (
        tmp_path / "sales.csv"
    )

    file_path.write_text(
        "sample",
        encoding="utf-8",
    )

    validator.validate_file_exists(
        str(file_path)
    )


def test_validate_file_exists_failure(
    validator,
):
    with pytest.raises(
        FileNotFoundError,
        match="Source file not found",
    ):
        validator.validate_file_exists(
            "missing.csv"
        )


def test_validate_not_empty_success(
    validator,
    tmp_path,
):
    file_path = (
        tmp_path / "sales.csv"
    )

    file_path.write_text(
        "data",
        encoding="utf-8",
    )

    validator.validate_not_empty(
        str(file_path)
    )


def test_validate_not_empty_failure(
    validator,
    tmp_path,
):
    file_path = (
        tmp_path / "empty.csv"
    )

    file_path.write_text(
        "",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Input file is empty",
    ):
        validator.validate_not_empty(
            str(file_path)
        )


def test_validate_header_success(
    validator,
):
    validator.validate_header(
        list(EXPECTED_COLUMNS)
    )


def test_validate_header_failure(
    validator,
):
    with pytest.raises(
        ValueError,
        match="Header validation failed",
    ):
        validator.validate_header(
            [
                "wrong_column",
                "order_id",
            ]
        )