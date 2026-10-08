from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sales_etl.config import (
    DEFAULT_APP_NAME,
    DEFAULT_BASE_PATH,
    DEFAULT_INPUT_PATH,
    DEFAULT_LOG_DIR,
)
from sales_etl.pipelines.bronze import BronzePipeline
from sales_etl.pipelines.gold import GoldPipeline
from sales_etl.pipelines.silver import SilverPipeline
from sales_etl.utils.logging_config import configure_logging
from sales_etl.utils.spark_session import SparkSessionFactory
from sales_etl.utils.storage import join_storage_path


class SalesETLPipeline:
    """Orchestrate the sales data through Bronze, Silver, and Gold layers."""

    def __init__(
        self,
        base_path: str | Path = DEFAULT_BASE_PATH,
        app_name: str = DEFAULT_APP_NAME,
        input_path: str | Path | None = None,
        log_dir: str | Path = DEFAULT_LOG_DIR,
    ) -> None:
        self.base_path = str(base_path)
        self.input_path = input_path or DEFAULT_INPUT_PATH
        self.app_name = app_name

        self.logger = configure_logging(log_dir)

        self.spark: SparkSession = (
            SparkSessionFactory(app_name).create()
        )

        self.bronze_path = join_storage_path(
            self.base_path,
            "bronze",
            "raw_sales",
        )

        self.silver_path = join_storage_path(
            self.base_path,
            "silver",
            "sales",
        )

        self.gold_path = join_storage_path(
            self.base_path,
            "gold",
        )

        self.bronze_pipeline = BronzePipeline(
            spark=self.spark,
            output_path=self.bronze_path,
            logger=self.logger,
        )

        self.silver_pipeline = SilverPipeline(
            output_path=self.silver_path,
            logger=self.logger,
        )

        self.gold_pipeline = GoldPipeline(
            output_path=self.gold_path,
            logger=self.logger,
        )

    def run(self) -> dict[str, DataFrame]:
        """Execute the end-to-end ETL pipeline."""

        self.logger.info("=" * 80)
        self.logger.info("Sales ETL Pipeline Started")
        self.logger.info("=" * 80)

        try:
            # ==================================================
            # Bronze Layer
            # ==================================================

            self.logger.info(
                "Starting Bronze layer processing."
            )

            bronze_source_df = self.bronze_pipeline.read_source(
                self.input_path
            )

            self.bronze_pipeline.write(
                bronze_source_df
            )

            bronze_df = self.bronze_pipeline.read()

            self.logger.info(
                "Bronze layer processing completed successfully."
            )

            # ==================================================
            # Silver Layer
            # ==================================================

            self.logger.info(
                "Starting Silver layer processing."
            )

            silver_df, silver_quality_df = (
                self.silver_pipeline.run(
                    bronze_df
                )
            )

            self.logger.info(
                "Silver layer processing completed successfully."
            )            

            # ==================================================
            # Gold Layer
            # ==================================================

            self.logger.info(
                "Starting Gold layer processing."
            )

            result = self.gold_pipeline.run(
                silver_df,
                silver_quality_df,
            )

            self.logger.info(
                "Gold layer processing completed successfully."
            )

            self.logger.info("" * 80)
            self.logger.info(
                "Sales ETL Pipeline Completed Successfully"
            )
            self.logger.info("=" * 80)

            return result

        except Exception as exc:
            self.logger.exception(
                "Sales ETL Pipeline failed. Error=%s",
                str(exc),
            )
            raise

    def close(self) -> None:
        """Stop Spark session and release resources."""

        self.logger.info(
            "Stopping Spark session."
        )

        try:
            self.spark.stop()

            self.logger.info(
                "Spark session stopped successfully."
            )

        except Exception as exc:
            self.logger.exception(
                "Error while stopping Spark session. Error=%s",
                str(exc),
            )
            raise