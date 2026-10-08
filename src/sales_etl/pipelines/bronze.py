from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.transformations.sales_transform import (
    add_order_date_partitions,
    set_layer_metadata,
)
from sales_etl.utils.storage import storage_path_exists
from sales_etl.validations.file_validator import FileValidator

BRONZE_SCHEMA = StructType(
    [
        StructField("row_id", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("ship_date", StringType(), False),
        StructField("ship_mode", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
    ]
)

BRONZE_COLUMNS = (
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
)


class BronzePipeline:
    """Bronze layer ingestion pipeline."""

    def __init__(
        self,
        spark: SparkSession,
        output_path: str | Path,
        logger: logging.Logger,
    ) -> None:
        self.spark = spark
        self.output_path = output_path
        self.logger = logger
        self.file_validator = FileValidator(logger=self.logger)

    def read_source(self, input_path: str | Path) -> DataFrame:
        """Read source CSV after performing file-level validations."""

        path = str(input_path)

        self.logger.info("Starting source file validation.")

        self.file_validator.validate_file_exists(path)
        self.file_validator.validate_not_empty(path)

        self.logger.info("Reading source file from %s", path)

        source_df = (
            self.spark.read.schema(BRONZE_SCHEMA)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(path)
        )

        self.file_validator.validate_header(source_df.columns)

        self.logger.info(
            "Source file successfully loaded. Record count=%s",
            source_df.count(),
        )

        source_df = add_order_date_partitions(source_df)
        source_df = set_layer_metadata(source_df, self.output_path)

        self.logger.info("Metadata columns added successfully.")

        return source_df

    def write(self, df: DataFrame) -> DataFrame:
        """Write data to Bronze layer."""

        self.logger.info("Writing Bronze dataset.")

        bronze_df = set_layer_metadata(
            df.select(*BRONZE_COLUMNS),
            self.output_path,
        )

        self.logger.info(
            "Bronze dataset record count=%s",
            bronze_df.count(),
        )

        (
            bronze_df.write.mode("overwrite")
            .partitionBy(*DATE_PARTITION_COLUMNS)
            .parquet(str(self.output_path))
        )

        self.logger.info(
            "Bronze dataset written successfully to %s",
            self.output_path,
        )

        return bronze_df

    def ingest(self, input_path: str | Path) -> DataFrame:
        """Read source data and write to Bronze layer."""

        self.logger.info("Starting Bronze ingestion.")

        bronze_df = self.write(self.read_source(input_path))

        self.logger.info("Bronze ingestion completed.")

        return bronze_df

    def read(self) -> DataFrame:
        """Read Bronze dataset."""

        if not storage_path_exists(self.spark, self.output_path):
            raise FileNotFoundError(
                f"Bronze dataset not found at {self.output_path}"
            )

        self.logger.info(
            "Reading Bronze dataset from %s",
            self.output_path,
        )

        bronze_df = self.spark.read.parquet(str(self.output_path))

        self.logger.info("Bronze dataset loaded successfully.")

        return bronze_df