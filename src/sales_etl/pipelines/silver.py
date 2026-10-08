from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.transformations.data_quality import (
    build_data_quality_report,
    split_valid_invalid_records,
)
from sales_etl.transformations.sales_transform import (
    set_layer_metadata,
    transform_sales_to_silver,
)


class SilverPipeline:
    """Silver layer processing pipeline."""

    def __init__(
        self,
        output_path: str | Path,
        logger: logging.Logger,
    ) -> None:
        self.output_path = output_path
        self.logger = logger

        self.rejected_path = (
            Path(output_path).parent
            / "rejected_records"
        )

    def transform(
        self,
        bronze_df: DataFrame,
    ) -> DataFrame:
        """Apply Silver layer standardization."""

        self.logger.info(
            "Starting Silver data standardization."
        )

        return transform_sales_to_silver(bronze_df)

    def write(
        self,
        silver_df: DataFrame,
    ) -> DataFrame:
        """Write valid records to Silver."""

        silver_df = set_layer_metadata(
            silver_df,
            self.output_path,
        )

        (
            silver_df.write
            .mode("overwrite")
            .partitionBy(*DATE_PARTITION_COLUMNS)
            .parquet(str(self.output_path))
        )

        self.logger.info(
            "Silver dataset written to %s",
            self.output_path,
        )

        return silver_df

    def write_rejected_records(
        self,
        rejected_df: DataFrame,
    ) -> None:
        """Write rejected records to quarantine area."""

        if rejected_df.rdd.isEmpty():
            self.logger.info(
                "No rejected records found."
            )
            return

        (
            rejected_df.write
            .mode("overwrite")
            .parquet(str(self.rejected_path))
        )

        self.logger.info(
            "Rejected records written to %s",
            self.rejected_path,
        )

    def run(
        self,
        bronze_df: DataFrame,
    ) -> tuple[DataFrame, DataFrame]:
        """
        Execute Silver processing.

        Returns:
            tuple(
                silver_df,
                quality_report_df
            )
        """

        transformed_df = self.transform(
            bronze_df
        )

        valid_df, rejected_df = (
            split_valid_invalid_records(
                transformed_df
            )
        )

        self.logger.info(
            "Valid record count=%s",
            valid_df.count(),
        )

        self.logger.info(
            "Rejected record count=%s",
            rejected_df.count(),
        )

        self.write_rejected_records(
            rejected_df
        )

        silver_df = self.write(
            valid_df
        )

        quality_df = build_data_quality_report(
            transformed_df,
            self.output_path,
            layer="silver",
        )

        self.logger.info(
            "Silver data quality report generated."
        )

        return silver_df, quality_df
