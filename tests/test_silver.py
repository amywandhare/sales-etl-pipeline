from pathlib import Path

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.transformations.data_quality import (
    split_valid_invalid_records,
)
from sales_etl.utils.storage import metadata_path


def test_silver_standardizes_shipment_fields_and_metadata(
    pipeline,
):
    bronze_df = (
        pipeline.bronze_pipeline.ingest(
            pipeline.input_path
        )
    )

    silver_df, quality_df = (
        pipeline.silver_pipeline.run(
            bronze_df
        )
    )

    assert "shipment_date" in silver_df.columns
    assert "shipment_mode" in silver_df.columns

    assert "ship_date" not in silver_df.columns
    assert "ship_mode" not in silver_df.columns

    assert (
        set(DATE_PARTITION_COLUMNS)
        <= set(silver_df.columns)
    )

    assert (
        silver_df.select("file_path")
        .first()[0]
        == metadata_path(
            pipeline.silver_path
        )
    )

    assert (
        silver_df.select(
            "execution_datetime"
        )
        .first()[0]
        .endswith("Z")
    )

    # Silver should contain all valid records
    assert silver_df.count() > 0

    assert quality_df.count() > 0


def test_silver_writes_valid_records(
    pipeline,
):
    bronze_df = (
        pipeline.bronze_pipeline.ingest(
            pipeline.input_path
        )
    )

    transformed_df = (
        pipeline.silver_pipeline.transform(
            bronze_df
        )
    )

    valid_df, _ = (
        split_valid_invalid_records(
            transformed_df
        )
    )

    pipeline.silver_pipeline.write(
        valid_df
    )

    silver_path = Path(
        pipeline.silver_path
    )

    assert silver_path.exists()

    assert list(
        silver_path.glob(
            "order_date_year=*/"
            "order_date_month=*"
        )
    )


def test_write_rejected_records_with_empty_dataframe(
    pipeline,
):
    bronze_df = (
        pipeline.bronze_pipeline.ingest(
            pipeline.input_path
        )
    )

    transformed_df = (
        pipeline.silver_pipeline.transform(
            bronze_df
        )
    )

    _, rejected_df = (
        split_valid_invalid_records(
            transformed_df
        )
    )

    assert rejected_df.count() == 0

    pipeline.silver_pipeline.write_rejected_records(
        rejected_df
    )

    assert not Path(
        pipeline.silver_pipeline.rejected_path
    ).exists()


def test_write_rejected_records_with_invalid_data(
    pipeline,
):
    bronze_df = (
        pipeline.bronze_pipeline.ingest(
            pipeline.input_path
        )
    )

    transformed_df = (
        pipeline.silver_pipeline.transform(
            bronze_df
        )
    )

    bad_df = (
        transformed_df.limit(1)
        .withColumn(
            "shipment_date",
            transformed_df.order_date,
        )
        .withColumn(
            "shipment_mode",
            transformed_df.shipment_mode,
        )
    )

    pipeline.silver_pipeline.write_rejected_records(
        bad_df
    )

    rejected_path = Path(
        pipeline.silver_pipeline.rejected_path
    )

    assert rejected_path.exists()


def test_silver_transform_returns_dataframe(
    pipeline,
):
    bronze_df = (
        pipeline.bronze_pipeline.ingest(
            pipeline.input_path
        )
    )

    silver_df = (
        pipeline.silver_pipeline.transform(
            bronze_df
        )
    )

    # Transformation should preserve source record count
    assert (
        silver_df.count()
        == bronze_df.count()
    )

    assert {
        "shipment_date",
        "shipment_mode",
        "customer_name",
        "country",
    } <= set(silver_df.columns)