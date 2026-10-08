from pathlib import Path

import pytest

from sales_etl.pipelines.bronze import BronzePipeline
from sales_etl.utils.storage import join_storage_path


def test_missing_bronze_dataset_raises(
    pipeline,
):
    missing_pipeline = BronzePipeline(
        spark=pipeline.spark,
        output_path=join_storage_path(
            pipeline.base_path,
            "missing",
        ),
        logger=pipeline.logger,
    )

    with pytest.raises(
        FileNotFoundError,
        match="Bronze dataset not found",
    ):
        missing_pipeline.read()


def test_missing_source_csv_raises(
    pipeline,
    tmp_path,
):
    missing_path = (
        tmp_path
        / "missing.csv"
    )

    with pytest.raises(
        FileNotFoundError
    ):
        pipeline.bronze_pipeline.read_source(
            missing_path
        )


def test_empty_source_csv_raises(
    pipeline,
    tmp_path,
):
    empty_file = (
        tmp_path
        / "empty.csv"
    )

    empty_file.write_text(
        "",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
    ):
        pipeline.bronze_pipeline.read_source(
            empty_file
        )