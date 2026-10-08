from pathlib import Path

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.utils.storage import (
    join_storage_path,
    metadata_path,
)


def _silver_snapshot(pipeline):
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

    return silver_df, quality_df


def test_gold_sales_and_customer_products(
    pipeline,
):
    silver_df, quality_df = (
        _silver_snapshot(
            pipeline
        )
    )

    outputs = pipeline.gold_pipeline.run(
        silver_df,
        quality_df,
    )

    sales_df = outputs["sales"]
    customer_df = outputs["customer"]
    dq_df = outputs["data_quality"]

    assert set(outputs.keys()) == {
        "sales",
        "customer",
        "data_quality",
    }

    assert sales_df.count() == 8
    assert customer_df.count() > 0
    assert dq_df.count() > 0

    assert {
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_mode",
    } <= set(sales_df.columns)

    assert {
        "customer_id",
        "orders_last_month",
        "orders_last_6_months",
    } <= set(customer_df.columns)

    assert {
        "layer",
        "rule_name",
        "status",
    } <= set(dq_df.columns)

    assert (
        set(DATE_PARTITION_COLUMNS)
        <= set(sales_df.columns)
    )

    assert (
        set(DATE_PARTITION_COLUMNS)
        <= set(customer_df.columns)
    )

    assert (
        sales_df.select(
            "file_path"
        ).first()[0]
        == metadata_path(
            join_storage_path(
                pipeline.gold_path,
                "sales",
            )
        )
    )

    assert (
        customer_df.select(
            "file_path"
        ).first()[0]
        == metadata_path(
            join_storage_path(
                pipeline.gold_path,
                "customer",
            )
        )
    )


def test_gold_datasets_written_to_storage(
    pipeline,
):
    silver_df, quality_df = (
        _silver_snapshot(
            pipeline
        )
    )

    pipeline.gold_pipeline.run(
        silver_df,
        quality_df,
    )

    sales_path = Path(
        join_storage_path(
            pipeline.gold_path,
            "sales",
        )
    )

    customer_path = Path(
        join_storage_path(
            pipeline.gold_path,
            "customer",
        )
    )

    quality_path = Path(
        join_storage_path(
            pipeline.gold_path,
            "data_quality",
        )
    )

    assert sales_path.exists()
    assert customer_path.exists()
    assert quality_path.exists()


def test_customer_gold_is_overwritten_on_each_run(
    pipeline,
):
    silver_df, quality_df = (
        _silver_snapshot(
            pipeline
        )
    )

    first_outputs = (
        pipeline.gold_pipeline.run(
            silver_df,
            quality_df,
        )
    )

    first_timestamp = (
        first_outputs["customer"]
        .select(
            "execution_datetime"
        )
        .first()[0]
    )

    second_outputs = (
        pipeline.gold_pipeline.run(
            silver_df,
            quality_df,
        )
    )

    second_timestamp = (
        second_outputs["customer"]
        .select(
            "execution_datetime"
        )
        .first()[0]
    )

    assert (
        first_timestamp
        != second_timestamp
    )


def test_gold_sales_partitions_created(
    pipeline,
):
    silver_df, quality_df = (
        _silver_snapshot(
            pipeline
        )
    )

    pipeline.gold_pipeline.run(
        silver_df,
        quality_df,
    )

    sales_path = Path(
        join_storage_path(
            pipeline.gold_path,
            "sales",
        )
    )

    assert list(
        sales_path.glob(
            "order_date_year=*/"
            "order_date_month=*"
        )
    )


def test_gold_customer_partitions_created(
    pipeline,
):
    silver_df, quality_df = (
        _silver_snapshot(
            pipeline
        )
    )

    pipeline.gold_pipeline.run(
        silver_df,
        quality_df,
    )

    customer_path = Path(
        join_storage_path(
            pipeline.gold_path,
            "customer",
        )
    )

    assert list(
        customer_path.glob(
            "order_date_year=*/"
            "order_date_month=*"
        )
    )