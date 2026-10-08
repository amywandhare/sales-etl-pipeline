import os
import sys

import pytest
from pyspark.sql import SparkSession

from sales_etl.etl_pipeline import SalesETLPipeline
from sales_etl.utils.spark_session import SparkSessionFactory


@pytest.fixture
def spark_session():
    spark = (
        SparkSession.builder
        .appName("sales_etl_transform_tests")
        .master("local[1]")
        .config(
            "spark.sql.shuffle.partitions",
            "1",
        )
        .config(
            "spark.pyspark.python",
            sys.executable,
        )
        .config(
            "spark.pyspark.driver.python",
            sys.executable,
        )
        .config(
            "spark.executorEnv.PYSPARK_PYTHON",
            sys.executable,
        )
        .config(
            "spark.executorEnv.PYSPARK_DRIVER_PYTHON",
            sys.executable,
        )
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def pipeline(tmp_path):
    if os.name == "nt":
        try:
            SparkSessionFactory._configure_windows_hadoop()
        except RuntimeError as error:
            pytest.skip(str(error))

    etl = SalesETLPipeline(
        base_path=tmp_path / "datalake",
        log_dir=tmp_path / "logs",
        app_name="test_pipeline",
    )

    yield etl

    etl.close()
