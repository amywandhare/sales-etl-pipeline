import csv
from pathlib import Path

import pytest

from sales_etl.config import (
    DATE_PARTITION_COLUMNS,
    DEFAULT_INPUT_PATH,
)
from sales_etl.pipelines.bronze import BronzePipeline
from sales_etl.utils.storage import metadata_path


def test_sample_csv_matches_assignment_schema():
    with DEFAULT_INPUT_PATH.open(
        newline="",
        encoding="utf-8",
    ) as source_file:
        reader = csv.DictReader(source_file)
        rows = list(reader)

    assert reader.fieldnames == [
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "city",
    ]

    assert len(rows) == 8


def test_bronze_ingests_csv_with_metadata_and_partitions(
    pipeline,
):
    bronze_df = pipeline.bronze_pipeline.ingest(
        pipeline.input_path
    )

    assert bronze_df.count() == 8

    assert {
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "city",
        "file_path",
        "execution_datetime",
        *DATE_PARTITION_COLUMNS,
    } <= set(bronze_df.columns)

    assert (
        bronze_df.schema["order_date"]
        .dataType
        .simpleString()
        == "string"
    )

    assert (
        bronze_df.schema["ship_date"]
        .dataType
        .simpleString()
        == "string"
    )

    assert (
        bronze_df.select("file_path")
        .first()[0]
        == metadata_path(
            pipeline.bronze_path
        )
    )

    assert (
        bronze_df.select(
            "execution_datetime"
        )
        .first()[0]
        .endswith("Z")
    )

    assert list(
        Path(
            pipeline.bronze_path
        ).glob(
            "order_date_year=*/"
            "order_date_month=*/"
            "order_date_day=*"
        )
    )


def test_bronze_read_returns_written_dataset(
    pipeline,
):
    written_df = (
        pipeline.bronze_pipeline.ingest(
            pipeline.input_path
        )
    )

    read_df = (
        pipeline.bronze_pipeline.read()
    )

    assert read_df.count() == written_df.count()

    assert {
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "city",
    } <= set(read_df.columns)


def test_bronze_read_raises_when_dataset_missing(
    pipeline,
):
    missing_pipeline = BronzePipeline(
        spark=pipeline.spark,
        output_path="missing_bronze_dataset",
        logger=pipeline.logger,
    )

    with pytest.raises(
        FileNotFoundError,
        match="Bronze dataset not found",
    ):
        missing_pipeline.read()


def test_orchestrator_runs_all_layers_from_sample_csv(
    pipeline,
):
    outputs = pipeline.run()

    assert set(outputs.keys()) == {
        "sales",
        "customer",
        "data_quality",
    }

    sales_df = outputs["sales"]
    customer_df = outputs["customer"]
    quality_df = outputs["data_quality"]

    assert sales_df.count() > 0
    assert customer_df.count() > 0
    assert quality_df.count() > 0

    assert {
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_mode",
        "city",
    } <= set(sales_df.columns)

    assert {
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_segment",
        "country",
        "orders_last_month",
        "orders_last_6_months",
    } <= set(customer_df.columns)

    assert {
        "layer",
        "rule_name",
        "status",
        "checked_records",
        "failed_records",
    } <= set(quality_df.columns)