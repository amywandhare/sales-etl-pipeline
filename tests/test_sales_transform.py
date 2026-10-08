from pathlib import Path

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.transformations.sales_transform import (
    add_order_date_partitions,
    build_sales_gold,
    execution_timestamp,
    set_layer_metadata,
    transform_sales_to_silver,
)


BRONZE_SCHEMA = StructType(
    [
        StructField("row_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("execution_datetime", StringType(), True),
    ]
)


def test_execution_timestamp_returns_utc_format():
    timestamp = execution_timestamp()

    assert timestamp.endswith("Z")
    assert "T" in timestamp


def test_add_order_date_partitions(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                "2025-01-15",
            )
        ],
        ["order_date"],
    )

    result = add_order_date_partitions(
        df
    )

    row = result.first()

    assert row.order_date_year == 2025
    assert row.order_date_month == 1
    assert row.order_date_day == 15


def test_set_layer_metadata(
    spark_session,
):
    df = spark_session.createDataFrame(
        [(1,)],
        ["id"],
    )

    result = set_layer_metadata(
        df,
        Path("silver/sales"),
    )

    row = result.first()

    assert row.file_path is not None
    assert row.execution_datetime.endswith(
        "Z"
    )


def test_transform_sales_to_silver(
    spark_session,
):
    bronze_df = spark_session.createDataFrame(
        [
            (
                "1",
                "ORD001",
                "2025-01-01",
                "2025-01-02",
                "first class",
                "C001",
                "  john    doe  ",
                "consumer",
                "india",
                " pune ",
                "/tmp/test",
                "2025-01-01T00:00:00Z",
            )
        ],
        BRONZE_SCHEMA,
    )

    silver_df = (
        transform_sales_to_silver(
            bronze_df
        )
    )

    row = silver_df.first()

    assert "ship_date" not in silver_df.columns
    assert "ship_mode" not in silver_df.columns

    assert (
        "shipment_date"
        in silver_df.columns
    )

    assert (
        "shipment_mode"
        in silver_df.columns
    )

    assert row.customer_name == "John Doe"
    assert row.country == "INDIA"
    assert row.segment == "CONSUMER"

    assert (
        row.shipment_mode
        == "First Class"
    )

    assert row.order_date is not None
    assert row.shipment_date is not None

    assert (
        set(DATE_PARTITION_COLUMNS)
        <= set(silver_df.columns)
    )


def test_transform_sales_to_silver_blank_strings_become_null(
    spark_session,
):
    bronze_df = spark_session.createDataFrame(
        [
            (
                "1",
                "ORD001",
                "2025-01-01",
                "2025-01-02",
                "",
                "C001",
                "",
                "consumer",
                "india",
                "",
                "/tmp/test",
                "2025-01-01T00:00:00Z",
            )
        ],
        BRONZE_SCHEMA,
    )

    silver_df = (
        transform_sales_to_silver(
            bronze_df
        )
    )

    row = silver_df.first()

    assert row.customer_name is None
    assert row.city is None
    assert row.shipment_mode is None


def test_build_sales_gold(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                "ORD001",
                "2025-01-01",
                "2025-01-02",
                "First Class",
                "PUNE",
                "/tmp/test",
                "2025-01-01T00:00:00Z",
                2025,
                1,
                1,
            )
        ],
        [
            "order_id",
            "order_date",
            "shipment_date",
            "shipment_mode",
            "city",
            "file_path",
            "execution_datetime",
            "order_date_year",
            "order_date_month",
            "order_date_day",
        ],
    )

    gold_df = build_sales_gold(df)

    assert gold_df.count() == 1

    assert {
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_mode",
        "city",
        "file_path",
        "execution_datetime",
        *DATE_PARTITION_COLUMNS,
    } <= set(gold_df.columns)