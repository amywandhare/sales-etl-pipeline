from datetime import date

import pytest
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.transformations.customer_transform import (
    _month_shift,
    build_customer_gold,
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("country", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("order_date", DateType(), True),
    ]
)


def test_month_shift_back_11_months():
    result = _month_shift(
        date(2018, 12, 20),
        -11,
    )

    assert result == date(2018, 1, 1)


def test_month_shift_back_5_months():
    result = _month_shift(
        date(2018, 12, 20),
        -5,
    )

    assert result == date(2018, 7, 1)


def test_build_customer_gold(
    spark_session,
):
    silver_df = spark_session.createDataFrame(
        [
            (
                "C-10001",
                "Alice Smith",
                "CONSUMER",
                "INDIA",
                "ORD001",
                date(2018, 12, 1),
            ),
            (
                "C-10001",
                "Alice Smith",
                "CONSUMER",
                "INDIA",
                "ORD002",
                date(2018, 12, 10),
            ),
            (
                "C-10001",
                "Alice Smith",
                "CONSUMER",
                "INDIA",
                "ORD003",
                date(2018, 12, 20),
            ),
        ],
        schema=CUSTOMER_SCHEMA,
    )

    result = build_customer_gold(
        silver_df
    )

    assert result.count() == 1

    row = result.first()

    assert row.customer_id == "C-10001"
    assert row.customer_first_name == "Alice"
    assert row.customer_last_name == "Smith"
    assert row.customer_segment == "CONSUMER"
    assert row.country == "INDIA"

    assert row.orders_last_month == 3
    assert row.orders_last_6_months == 3
    assert row.orders_last_12_months == 3
    assert row.total_orders == 3

    assert row.order_date_year == 2018
    assert row.order_date_month == 12
    assert row.order_date_day == 20

    assert {
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
    } <= set(result.columns)


def test_customer_gold_counts_distinct_orders(
    spark_session,
):
    silver_df = spark_session.createDataFrame(
        [
            (
                "C-10001",
                "Alice Smith",
                "CONSUMER",
                "INDIA",
                "ORD001",
                date(2018, 12, 20),
            ),
            (
                "C-10001",
                "Alice Smith",
                "CONSUMER",
                "INDIA",
                "ORD001",
                date(2018, 12, 20),
            ),
        ],
        schema=CUSTOMER_SCHEMA,
    )

    result = build_customer_gold(
        silver_df
    )

    row = result.first()

    assert row.total_orders == 1


def test_customer_gold_splits_name_correctly(
    spark_session,
):
    silver_df = spark_session.createDataFrame(
        [
            (
                "C-1",
                "John Doe",
                "CONSUMER",
                "INDIA",
                "ORD001",
                date(2018, 12, 20),
            ),
        ],
        schema=CUSTOMER_SCHEMA,
    )

    result = build_customer_gold(
        silver_df
    )

    row = result.first()

    assert row.customer_first_name == "John"
    assert row.customer_last_name == "Doe"


def test_build_customer_gold_raises_for_empty_dataset(
    spark_session,
):
    empty_df = spark_session.createDataFrame(
        [],
        schema=CUSTOMER_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="valid order_dates",
    ):
        build_customer_gold(
            empty_df
        )