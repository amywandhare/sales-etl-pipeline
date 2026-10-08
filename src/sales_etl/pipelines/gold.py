from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame

from sales_etl.config import DATE_PARTITION_COLUMNS
from sales_etl.transformations.customer_transform import (
    build_customer_gold,
)
from sales_etl.transformations.sales_transform import (
    build_sales_gold,
    set_layer_metadata,
)
from sales_etl.utils.storage import join_storage_path


class GoldPipeline:
    """Gold layer processing pipeline."""

    def __init__(
        self,
        output_path: str | Path,
        logger: logging.Logger,
    ) -> None:
        self.output_path = output_path
        self.logger = logger

    def run(
        self,
        silver_df: DataFrame,
        quality_df: DataFrame,
    ) -> dict[str, DataFrame    ]:
        """Generate and persist Gold datasets."""

        sales_path = join_storage_path(
            self.output_path,
            "sales",
        )

        customer_path = join_storage_path(
            self.output_path,
            "customer",
        )

        quality_path = join_storage_path(
            self.output_path,
            "data_quality",
        )

        self.logger.info(
            "Building Gold sales dataset."
        )

        sales_df = set_layer_metadata(
            build_sales_gold(silver_df),
            sales_path,
        )

        self.logger.info(
            "Building Gold customer dataset."
        )

        customer_df = set_layer_metadata(
            build_customer_gold(silver_df),
            customer_path,
        )

        # --------------------------------------------------
        # Sales Gold
        # --------------------------------------------------

        (
            sales_df.write
            .mode("overwrite")
            .partitionBy(*DATE_PARTITION_COLUMNS)
            .parquet(str(sales_path))
        )

        self.logger.info(
            "Gold sales dataset written to %s",
            sales_path,
        )

        # --------------------------------------------------
        # Customer Gold
        # --------------------------------------------------

        (
            customer_df.write
            .mode("overwrite")
            .partitionBy(*DATE_PARTITION_COLUMNS)
            .parquet(str(customer_path))
        )

        self.logger.info(
            "Gold customer dataset written to %s",
            customer_path,
        )

        # --------------------------------------------------
        # Data Quality Report
        # --------------------------------------------------

        (
            quality_df.write
            .mode("overwrite")
            .parquet(str(quality_path))
        )

        self.logger.info(
            "Gold data quality report written to %s",
            quality_path,
        )

        return {
            "sales": sales_df,
            "customer": customer_df,
            "data_quality": quality_df,
        }