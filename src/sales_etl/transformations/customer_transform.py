from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_etl.config import DATE_PARTITION_COLUMNS


def _month_shift(
    date_value: date,
    months: int,
) -> date:
    year = date_value.year + (
        (date_value.month - 1 + months) // 12
    )
    month = (
        (date_value.month - 1 + months) % 12
    ) + 1

    return date_value.replace(
        year=year,
        month=month,
        day=1,
    )


def build_customer_gold(
    silver_df: DataFrame,
) -> DataFrame:
    """Build customer Gold dataset."""

    latest_day = (
        silver_df
        .agg(
            F.max("order_date").alias("latest_day")
        )
        .first()["latest_day"]
    )

    if latest_day is None:
        raise ValueError(
            "The source data does not contain any valid order_dates."
        )

    # --------------------------------------------------
    # Rolling Date Windows
    # --------------------------------------------------

    last_month_start = latest_day - timedelta(days=30)

    last_6_month_start = _month_shift(
        latest_day,
        -5,
    )

    last_12_month_start = _month_shift(
        latest_day,
        -11,
    )

    # --------------------------------------------------
    # Customer Name Split
    # --------------------------------------------------

    customer_base = (
        silver_df
        .withColumn(
            "customer_first_name",
            F.split(
                F.col("customer_name"),
                " ",
            )[0],
        )
        .withColumn(
            "customer_last_name",
            F.element_at(
                F.split(
                    F.col("customer_name"),
                    " ",
                ),
                -1,
            ),
        )
    )

    # --------------------------------------------------
    # Customer Aggregations
    # --------------------------------------------------

    customer_counts = (
        customer_base
        .groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "segment",
            "country",
        )
        .agg(
            F.countDistinct(
                F.when(
                    F.col("order_date")
                    >= F.lit(last_month_start),
                    F.col("order_id"),
                )
            ).alias("orders_last_month"),
            F.countDistinct(
                F.when(
                    (
                        F.col("order_date")
                        >= F.lit(last_6_month_start)
                    )
                    & (
                        F.col("order_date")
                        <= F.lit(latest_day)
                    ),
                    F.col("order_id"),
                )
            ).alias("orders_last_6_months"),
            F.countDistinct(
                F.when(
                    (
                        F.col("order_date")
                        >= F.lit(last_12_month_start)
                    )
                    & (
                        F.col("order_date")
                        <= F.lit(latest_day)
                    ),
                    F.col("order_id"),
                )
            ).alias("orders_last_12_months"),
            F.countDistinct("order_id").alias(
                "total_orders"
            ),
        )
    )

    return (
        customer_counts
        .withColumnRenamed(
            "segment",
            "customer_segment",
        )
        .withColumn(
            "order_date_year",
            F.lit(latest_day.year),
        )
        .withColumn(
            "order_date_month",
            F.lit(latest_day.month),
        )
        .withColumn(
            "order_date_day",
            F.lit(latest_day.day),
        )
        .select(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_segment",
            "country",
            "orders_last_month",
            "orders_last_6_months",
            "orders_last_12_months",
            "total_orders",
            *DATE_PARTITION_COLUMNS,
        )
    )