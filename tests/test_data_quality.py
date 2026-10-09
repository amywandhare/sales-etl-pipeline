import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from sales_etl.transformations.data_quality import (
    build_data_quality_report,
    split_valid_invalid_records,
)

QUALITY_TEST_SCHEMA = StructType(
    [
        StructField("row_id", IntegerType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("city", StringType(), True),
    ]
)


def test_bronze_quality_checks_source_completeness_uniqueness_and_dates(
    spark_session,
    tmp_path,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "O-1",
                "2025-01-01",
                "2025-01-02",
                "First Class",
                "C-1",
                "Ada Lovelace",
                "INDIA",
                "CONSUMER",
                "PUNE",
            ),
            (
                2,
                "O-2",
                "2025-01-02",
                "2025-01-03",
                "Same Day",
                "C-2",
                "Grace Hopper",
                "INDIA",
                "CORPORATE",
                "MUMBAI",
            ),
        ],
        QUALITY_TEST_SCHEMA,
    )

    report = build_data_quality_report(
        df,
        tmp_path / "gold" / "data_quality",
        layer="bronze",
    )

    assert report.count() == 11
    assert report.filter("status = 'FAIL'").count() == 0
    assert report.select("checked_records").first()[0] == 2


def test_bronze_quality_counts_missing_values_bad_dates_and_duplicates(
    spark_session,
    tmp_path,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "O-1",
                "2025-01-02",
                "2025-01-03",
                "First Class",
                "C-1",
                "Ada Lovelace",
                "INDIA",
                "CONSUMER",
                "PUNE",
            ),
            (
                1,
                "O-1",
                "bad-date",
                "2025-01-01",
                "Overnight",
                None,
                "",
                None,
                "",
                "",
            ),
            (
                2,
                "O-2",
                "2025-01-02",
                "2025-01-01",
                "Same Day",
                "C-2",
                "Grace Hopper",
                "INDIA",
                "CORPORATE",
                "MUMBAI",
            ),
        ],
        QUALITY_TEST_SCHEMA,
    )

    report = build_data_quality_report(
        df,
        tmp_path / "gold" / "data_quality",
        layer="bronze",
    )

    failures = {
        row.rule_name: row.failed_records
        for row in report.filter(
            "status = 'FAIL'"
        ).collect()
    }

    assert failures["row_id_unique"] == 1
    assert failures["order_id_unique"] == 1
    assert failures["customer_id_not_null"] == 1
    assert failures["customer_name_not_null"] == 1
    assert failures["country_not_null"] == 1
    assert failures["segment_not_null"] == 1
    assert failures["city_not_null"] == 1
    assert failures["order_date_valid"] == 1


def test_silver_quality_checks_standardized_business_values(
    spark_session,
    tmp_path,
):
    bronze_df = spark_session.createDataFrame(
        [
            (
                1,
                "O-1",
                "2025-01-02",
                "2025-01-01",
                "First Class",
                "C-1",
                "Ada Lovelace",
                "INDIA",
                "CONSUMER",
                "PUNE",
            ),
            (
                2,
                "O-2",
                "2025-01-02",
                "2025-01-03",
                "Overnight",
                "C-2",
                "Grace Hopper",
                "INDIA",
                "CORPORATE",
                "MUMBAI",
            ),
        ],
        QUALITY_TEST_SCHEMA,
    )

    silver_df = (
        bronze_df
        .withColumnRenamed(
            "ship_date",
            "shipment_date",
        )
        .withColumnRenamed(
            "ship_mode",
            "shipment_mode",
        )
        .withColumn(
            "order_date",
            F.to_date("order_date"),
        )
        .withColumn(
            "shipment_date",
            F.to_date("shipment_date"),
        )
    )

    report = build_data_quality_report(
        silver_df,
        tmp_path / "gold" / "data_quality",
        layer="silver",
    )

    failures = {
        row.rule_name: row.failed_records
        for row in report.filter(
            "status = 'FAIL'"
        ).collect()
    }

    assert report.count() == 13

    assert (
        failures[
            "shipment_date_on_or_after_order_date"
        ]
        == 1
    )

    assert (
        failures[
            "shipment_mode_allowed"
        ]
        == 1
    )


def test_quality_report_rejects_invalid_layer(
    spark_session,
    tmp_path,
):
    df = spark_session.createDataFrame(
        [(1,)],
        ["id"],
    )

    with pytest.raises(
        ValueError,
        match="Data-quality layer",
    ):
        build_data_quality_report(
            df,
            tmp_path,
            layer="gold",
        )


def test_split_valid_invalid_records_all_valid(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "ORD001",
                "C001",
                "John Doe",
                "INDIA",
                "CONSUMER",
                "PUNE",
                "2025-01-01",
                "2025-01-02",
                "First Class",
            )
        ],
        [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "city",
            "order_date",
            "shipment_date",
            "shipment_mode",
        ],
    )

    valid_df, rejected_df = split_valid_invalid_records(df)

    assert valid_df.count() == 1
    assert rejected_df.count() == 0


def test_split_valid_invalid_records_duplicate_row_id(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "ORD001",
                "C001",
                "John Doe",
                "INDIA",
                "CONSUMER",
                "PUNE",
                "2025-01-01",
                "2025-01-02",
                "First Class",
            ),
            (
                1,
                "ORD002",
                "C002",
                "Jane Doe",
                "INDIA",
                "CONSUMER",
                "MUMBAI",
                "2025-01-01",
                "2025-01-02",
                "First Class",
            ),
        ],
        [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "city",
            "order_date",
            "shipment_date",
            "shipment_mode",
        ],
    )

    valid_df, rejected_df = split_valid_invalid_records(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 2


def test_split_valid_invalid_records_duplicate_order_id(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "ORD001",
                "C001",
                "John Doe",
                "INDIA",
                "CONSUMER",
                "PUNE",
                "2025-01-01",
                "2025-01-02",
                "First Class",
            ),
            (
                2,
                "ORD001",
                "C002",
                "Jane Doe",
                "INDIA",
                "CONSUMER",
                "MUMBAI",
                "2025-01-01",
                "2025-01-02",
                "First Class",
            ),
        ],
        [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "city",
            "order_date",
            "shipment_date",
            "shipment_mode",
        ],
    )

    valid_df, rejected_df = split_valid_invalid_records(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 2


def test_split_valid_invalid_records_invalid_ship_mode(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "ORD001",
                "C001",
                "John Doe",
                "INDIA",
                "CONSUMER",
                "PUNE",
                "2025-01-01",
                "2025-01-02",
                "Express",
            )
        ],
        [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "city",
            "order_date",
            "shipment_date",
            "shipment_mode",
        ],
    )

    valid_df, rejected_df = split_valid_invalid_records(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 1


def test_split_valid_invalid_records_ship_before_order(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "ORD001",
                "C001",
                "John Doe",
                "INDIA",
                "CONSUMER",
                "PUNE",
                "2025-01-10",
                "2025-01-01",
                "First Class",
            )
        ],
        [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "city",
            "order_date",
            "shipment_date",
            "shipment_mode",
        ],
    )

    valid_df, rejected_df = split_valid_invalid_records(df)

    assert valid_df.count() == 0
    assert rejected_df.count() == 1


def test_valid_and_rejected_counts_equal_source(
    spark_session,
):
    df = spark_session.createDataFrame(
        [
            (
                1,
                "ORD001",
                "C001",
                "John Doe",
                "INDIA",
                "CONSUMER",
                "PUNE",
                "2025-01-01",
                "2025-01-02",
                "First Class",
            )
        ],
        [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "city",
            "order_date",
            "shipment_date",
            "shipment_mode",
        ],
    )

    valid_df, rejected_df = split_valid_invalid_records(df)

    assert (
        valid_df.count()
        + rejected_df.count()
        == df.count()
    )