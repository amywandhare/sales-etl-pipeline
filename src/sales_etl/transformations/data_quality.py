from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from sales_etl.transformations.sales_transform import (
    execution_timestamp,
)
from sales_etl.utils.storage import metadata_path

QUALITY_REPORT_SCHEMA = StructType(
    [
        StructField("layer", StringType(), False),
        StructField("rule_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("checked_records", IntegerType(), False),
        StructField("failed_records", IntegerType(), False),
        StructField("rule_description", StringType(), False),
        StructField("file_path", StringType(), False),
        StructField("execution_datetime", StringType(), False),
    ]
)

ALLOWED_SHIP_MODES = (
    "First Class",
    "Second Class",
    "Standard Class",
    "Same Day",
)


def split_valid_invalid_records(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split records into valid and rejected datasets.

    Valid records are loaded into Silver.
    Rejected records are stored in the quarantine area.
    """

    duplicate_row_ids = (
        df.groupBy("row_id")
        .count()
        .filter(F.col("count") > 1)
        .select("row_id")
    )

    duplicate_order_ids = (
        df.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id")
    )

    valid_condition = (
        F.col("row_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("customer_name").isNotNull()
        & F.col("country").isNotNull()
        & F.col("segment").isNotNull()
        & F.col("city").isNotNull()
        & F.col("order_date").isNotNull()
        & F.col("shipment_date").isNotNull()
        & (
            F.col("shipment_date")
            >= F.col("order_date")
        )
        & F.col("shipment_mode").isin(*ALLOWED_SHIP_MODES)
    )

    valid_df = (
        df.join(
            duplicate_row_ids,
            on="row_id",
            how="left_anti",
        )
        .join(
            duplicate_order_ids,
            on="order_id",
            how="left_anti",
        )
        .filter(valid_condition)
    )

    rejected_df = (
        df.join(
            valid_df.select("row_id"),
            on="row_id",
            how="left_anti",
        )
    )       

    return valid_df, rejected_df


def build_data_quality_report(
    df: DataFrame,
    output_path: str | Path,
    layer: str,
) -> DataFrame:
    """Evaluate layer-specific rules and produce an audit report."""

    normalized_layer = layer.strip().lower()

    if normalized_layer not in {"bronze", "silver"}:
        raise ValueError(
            "Data-quality layer must be either 'bronze' or 'silver'."
        )

    blank_or_null = (
        lambda name:
        F.col(name).isNull()
        | (F.trim(F.col(name)) == "")
    )

    duplicate_count = (
        lambda name:
        F.count(F.col(name))
        - F.countDistinct(F.col(name))
    )

    expressions = [
        F.count(F.lit(1)).alias("checked_records")
    ]

    rules: list[tuple[str, str, str]] = []

    required_columns = (
        "row_id",
        "order_id",
        "customer_id",
        "customer_name",
        "country",
        "segment",
        "city",
    )

    for column_name in required_columns:

        alias = f"{column_name}_missing"

        expressions.append(
            F.sum(
                F.when(
                    blank_or_null(column_name),
                    1,
                ).otherwise(0)
            ).alias(alias)
        )

        rules.append(
            (
                f"{column_name}_not_null",
                alias,
                f"{column_name} must be present and not blank.",
            )
        )

    for column_name in (
        "row_id",
        "order_id",
    ):
        alias = f"{column_name}_duplicates"

        expressions.append(
            duplicate_count(column_name).alias(alias)
        )

        rules.append(
            (
                f"{column_name}_unique",
                alias,
                f"{column_name} must be unique across the input dataset.",
            )
        )

    ship_date_column = (
        "ship_date"
        if normalized_layer == "bronze"
        else "shipment_date"
    )

    expressions.extend(
        [
            F.sum(
                F.when(
                    F.to_date("order_date").isNull(),
                    1,
                ).otherwise(0)
            ).alias("order_date_invalid"),
            F.sum(
                F.when(
                    F.to_date(ship_date_column).isNull(),
                    1,
                ).otherwise(0)
            ).alias("ship_date_invalid"),
        ]
    )

    rules.extend(
        [
            (
                "order_date_valid",
                "order_date_invalid",
                "order_date must parse as a valid date.",
            ),
            (
                f"{ship_date_column}_valid",
                "ship_date_invalid",
                f"{ship_date_column} must parse as a valid date.",
            ),
        ]
    )

    if normalized_layer == "silver":

        expressions.extend(
            [
                F.sum(
                    F.when(
                        F.col("shipment_date")
                        < F.col("order_date"),
                        1,
                    ).otherwise(0)
                ).alias("ship_before_order"),
                F.sum(
                    F.when(
                        F.col("shipment_mode").isNull()
                        | ~F.col("shipment_mode").isin(
                            *ALLOWED_SHIP_MODES
                        ),
                        1,
                    ).otherwise(0)
                ).alias("ship_mode_invalid"),
            ]
        )

        rules.extend(
            [
                (
                    "shipment_date_on_or_after_order_date",
                    "ship_before_order",
                    "shipment_date must not be earlier than order_date.",
                ),
                (
                    "shipment_mode_allowed",
                    "ship_mode_invalid",
                    "shipment_mode must match an allowed canonical classification.",
                ),
            ]
        )

    metrics = df.agg(*expressions).first().asDict()

    checked_records = int(
        metrics["checked_records"] or 0
    )

    report_timestamp = execution_timestamp()

    report_path = metadata_path(output_path)

    report_rows = [
        (
            normalized_layer,
            rule_name,
            (
                "PASS"
                if int(metrics[failed_alias] or 0) == 0
                else "FAIL"
            ),
            checked_records,
            int(metrics[failed_alias] or 0),
            description,
            report_path,
            report_timestamp,
        )
        for rule_name, failed_alias, description in rules
    ]

    return df.sparkSession.createDataFrame(
        report_rows,
        schema=QUALITY_REPORT_SCHEMA,
    )