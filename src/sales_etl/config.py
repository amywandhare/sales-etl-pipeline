from pathlib import Path
from sysconfig import get_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "new_sales.csv"
_INSTALLED_INPUT_PATH = (
    Path(get_path("data")) / "share" / "sales-etl" / "data" / "raw" / "sales.csv"
)
DEFAULT_INPUT_PATH = next(
    (path for path in (_SOURCE_INPUT_PATH, _INSTALLED_INPUT_PATH) if path.exists()),
    _SOURCE_INPUT_PATH,
)
DEFAULT_BASE_PATH = Path("datalake")
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_APP_NAME = "sales_etl_pipeline"
DATE_PARTITION_COLUMNS = (
    "order_date_year",
    "order_date_month",
    "order_date_day",
)

EXPECTED_COLUMNS: tuple[str, ...] = (
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
)