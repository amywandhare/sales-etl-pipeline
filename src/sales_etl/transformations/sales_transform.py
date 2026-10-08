from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.utils.storage import metadata_path


def execution_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def add_order_date_partitions(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_date_year", F.year(F.to_date("order_date")))
        .withColumn("order_date_month", F.month(F.to_date("order_date")))
        .withColumn("order_date_day", F.dayofmonth(F.to_date("order_date")))
    )


def set_layer_metadata(df: DataFrame, output_path: str | Path) -> DataFrame:
    return (
        df.withColumn("file_path", F.lit(metadata_path(output_path)))
        .withColumn("execution_datetime", F.lit(execution_timestamp()))
    )


def transform_sales_to_silver(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Apply Silver layer standardization rules.
    """

    silver_df = (
        bronze_df.select(
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
        )
        .withColumnRenamed(
            "ship_date",
            "shipment_date",
        )
        .withColumnRenamed(
            "ship_mode",
            "shipment_mode",
        )
    )

    # --------------------------------------------------
    # Generic String Standardization
    # --------------------------------------------------

    string_columns = [
        field.name
        for field in silver_df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for column_name in string_columns:

        cleaned_value = F.regexp_replace(
            F.trim(F.col(column_name)),
            r"\s+",
            " ",
        )

        silver_df = silver_df.withColumn(
            column_name,
            F.when(
                cleaned_value == "",
                F.lit(None),
            ).otherwise(cleaned_value),
        )

    # --------------------------------------------------
    # Business Standardization
    # --------------------------------------------------

    silver_df = (
        silver_df
        .withColumn(
            "customer_name",
            F.initcap(F.col("customer_name")),
        )
        .withColumn(
            "country",
            F.upper(F.col("country")),
        )
        .withColumn(
            "segment",
            F.upper(F.col("segment")),
        )
    )

    # --------------------------------------------------
    # Date Conversion
    # --------------------------------------------------

    silver_df = (
        silver_df
        .withColumn(
            "order_date",
            F.to_date("order_date"),
        )
        .withColumn(
            "shipment_date",
            F.to_date("shipment_date"),
        )
    )

    # --------------------------------------------------
    # Shipment Mode Standardization
    # --------------------------------------------------

    normalized_mode = F.lower(
        F.col("shipment_mode")
    )

    silver_df = silver_df.withColumn(
        "shipment_mode",
        F.when(
            normalized_mode == "first class",
            "First Class",
        )
        .when(
            normalized_mode == "second class",
            "Second Class",
        )
        .when(
            normalized_mode == "standard class",
            "Standard Class",
        )
        .when(
            normalized_mode == "same day",
            "Same Day",
        )
        .otherwise(
            F.col("shipment_mode")
        ),
    )

    # --------------------------------------------------
    # Partition Columns
    # --------------------------------------------------

    silver_df = add_order_date_partitions(
        silver_df
    )

    return silver_df


def build_sales_gold(silver_df: DataFrame) -> DataFrame:
    return silver_df.select(
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_mode",
        "city",
        "file_path",
        "execution_datetime",
        *DATE_PARTITION_COLUMNS,
    )