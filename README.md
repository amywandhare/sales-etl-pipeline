# Sales Data ETL Pipeline

This project implements an installable PySpark ETL pipeline for a sample sales dataset. It writes Bronze, Silver, and Gold Parquet datasets, with separate Sales and Customer products in Gold.

## Features

- Multi-layer Data Lake architecture (Bronze, Silver, and Gold)
- PySpark-based ETL pipeline
- Parquet storage format for optimized analytics
- Partitioning by order year, month, and day
- Data quality validations and business rule enforcement
- Metadata tracking with `file_path` and `execution_datetime`
- Standardized snake_case column naming
- Reusable and modular transformation framework
- Configurable pipeline through centralized configuration
- Structured logging for monitoring, auditing, and debugging
- Comprehensive unit tests for transformations, pipelines, and error handling
- CLI interface for pipeline execution
- Installable Python package
- Docker support for containerized execution
- GitHub Actions CI pipeline for automated testing
- Sample sales dataset for end-to-end testing and demonstration


## Project Structure

```text
data/
└── raw/
    └── sales.csv              # Sample source data

src/
└── sales_etl/
    ├── config.py             # Pipeline configuration
    ├── cli.py                # Command-line entry point
    ├── etl_pipeline.py       # ETL orchestration
    ├── pipelines/            # Bronze, Silver, Gold layer logic
    ├── transformations/      # DataFrame transformations
    ├── validations/          # Data quality checks
    └── utils/                # Spark session, logging, storage helpers

tests/                        # Unit and integration tests

datalake/
├── bronze/
├── silver/
└── gold/                     # Generated output (gitignored)

logs/
└── pipeline.log              # Runtime logs (gitignored)

.github/workflows/            # CI/CD pipeline
Dockerfile                    # Containerization
```

## Setup

1. Create a virtual environment:
   - python -m venv .venv
2. Activate it:
   - Windows PowerShell: .\.venv\Scripts\Activate.ps1
3. Install the package in editable mode:
   - python -m pip install -e ".[test]"

   The sample CSV is included in the source tree and in built distributions. The default run reads `data/raw/sales.csv` and writes outputs to `datalake/`.

### Windows prerequisite

   The project pins PySpark 3.5.3 for reproducibility; its bundled Hadoop client is 3.3.4. Native Windows Parquet writes require genuine Hadoop Windows binaries built for Hadoop 3.3.4, including `winutils.exe` and its matching native DLLs. Set `HADOOP_HOME` to the directory containing the `bin` folder. Do not rename `cmd.exe` to `winutils.exe`; the pipeline detects and rejects that invalid workaround. A Linux environment is an alternative that does not require `winutils.exe`.

For example, in PowerShell:

```powershell
$env:HADOOP_HOME = 'C:\hadoop'
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

The Spark runtime also requires a supported Java installation (Java 8, 11, or 17 for Spark 3.5.x).

### Run locally with Docker (optional)

Docker runs the project in a Linux container with Python and Java installed, eliminating the Windows `winutils.exe` requirement. Install Docker Desktop and ensure Linux containers are enabled, then run the following commands from the project root in PowerShell:

```powershell
docker build -t sales-etl .
New-Item -ItemType Directory -Force -Path .\datalake, .\logs | Out-Null
docker run --rm `
   -v "${PWD}/datalake:/app/datalake" `
   -v "${PWD}/logs:/app/logs" `
   sales-etl
```

The container uses the packaged sample CSV by default. Parquet output is written to the host's `datalake/` directory and logs to `logs/pipeline.log`. To run the tests inside the container, build a test image with the test extra installed or run them in the local virtual environment on Linux/WSL.

## Run the Pipeline

From the project root, run the pipeline using the module entry point:

```powershell
python -m sales_etl
```

The pipeline uses the sample dataset included with the project:

```text
data/raw/sales.csv
```

and writes the processed data to:

```text
datalake/
├── bronze/
├── silver/
└── gold/
```

Runtime logs are written to:

```text
logs/
└── pipeline.log
```

You can also run the pipeline using the installed package entry point:

```powershell
sales-etl
```

To provide custom input and output locations:

```powershell
sales-etl --input-path data/raw/sales.csv --base-path datalake
```

### Expected Output Structure

```text
datalake/
├── bronze/
│   └── order_year=YYYY/
│       └── order_month=MM/
│           └── order_day=DD/
├── silver/
│   └── order_year=YYYY/
│       └── order_month=MM/
│           └── order_day=DD/
└── gold/
    └── order_year=YYYY/
        └── order_month=MM/
            └── order_day=DD/
```

Each layer is stored in Parquet format and partitioned by order year, month, and day. Additional metadata columns such as `file_path` and `execution_datetime` are added as records move through the pipeline.

## Run Tests

Run the full test suite:

```powershell
python -m pytest -q
```

Run the test suite with coverage:

```powershell
python -m pytest --cov=src/sales_etl --cov-report=term-missing
```

Run a specific test file:

```powershell
python -m pytest tests/test_bronze.py -v
```

Example with coverage for a single test file:

```powershell
python -m pytest tests/test_silver.py -v --cov=src/sales_etl --cov-report=term-missing
```

The test suite includes unit tests for:

- Bronze, Silver, and Gold pipeline layers
- Data transformations
- Data quality validations
- Error handling
- Spark session utilities
- ETL pipeline orchestration

On Windows, Parquet-related tests require compatible Hadoop Windows binaries (`winutils.exe`) for Hadoop 3.3.4. If the required binaries are not available, some Spark filesystem operations may fail. Running the tests in Linux, Docker, or WSL avoids this dependency.
``

## Data Products

- `bronze/raw_sales`: Source-shaped sales records loaded from the CSV file, including original date strings, ingestion metadata, and order-date partition columns derived without modifying source values.
- `silver/sales`: Standardized sales records with parsed dates, normalized column names, cleaned string values, canonical shipment field names, standardized ship modes, and layer-specific metadata.
- `gold/sales`: Curated order-level sales dataset for reporting and analytics.
- `gold/customer`: Customer-level aggregates containing order counts for the latest observed month, latest six calendar months, latest twelve calendar months, and all available history.
- `gold/data_quality`: Data quality audit dataset containing one record per validation rule, including PASS/FAIL status and checked/failed record counts.

Data quality validation is performed at both Bronze and Silver layers.

- **Bronze validations** verify required identifiers and names, uniqueness of `row_id` and `order_id`, and parseability of raw order and ship date strings while preserving the original source values.
- **Silver validations** verify required values, unique business keys, valid parsed dates, shipment-date chronology, and supported canonical ship modes after standardization.

The Gold data-quality report includes a `layer` column that identifies whether a validation result originated from the Bronze or Silver layer. Validation failures are reported in the audit dataset; records are not silently removed.

All datasets include the metadata columns:

- `file_path`
- `execution_datetime`

The `file_path` column identifies the dataset root for the corresponding layer, and `execution_datetime` records the UTC timestamp of the pipeline execution.

Customer aggregation windows are inclusive and anchored to the maximum `order_date` present in the source data, ensuring deterministic results for a given input snapshot.

All writes use overwrite mode, including the Customer Gold dataset.

Order-level datasets are partitioned by:

```text
order_date_year=YYYY/
order_date_month=M/
order_date_day=D/
```

Customer aggregates use the latest available source `order_date` to populate their partition values.

## Debugging

Pipeline logs are written to:

```text
logs/pipeline.log
```

and are also displayed in the console during execution.

You can specify a custom log directory:

```powershell
sales-etl --log-dir custom_logs
```

or

```powershell
python -m sales_etl --log-dir custom_logs
```

The ETL pipeline ensures that the Spark session is closed in a `finally` block, even when errors occur.

When troubleshooting:

1. Review `logs/pipeline.log` for detailed error messages.
2. Verify that a supported Java version is installed (Java 8, 11, or 17).
3. On Windows, confirm that `HADOOP_HOME` is configured correctly and that compatible Hadoop 3.3.4 Windows binaries are available.
4. Confirm that the expected Python virtual environment is active.
5. Check Spark startup messages before investigating transformation or data-quality logic.

Common issues include missing Hadoop binaries on Windows, incorrect Java configuration, invalid file paths, and insufficient permissions for writing to the `datalake/` or `logs/` directories.

## CI/CD

The project includes a GitHub Actions workflow located at:

```text
.github/workflows/ci.yml
```

The workflow is configured to run on:

- Pushes to the main branch
- Pull requests
- Manual workflow execution

The pipeline:

1. Sets up Python 3.11
2. Sets up Java 17
3. Installs project dependencies and test dependencies
4. Runs the full pytest test suite
5. Builds the Python package
6. Uploads build artifacts

The workflow runs on Linux, which allows PySpark and Parquet integration tests to execute without requiring Windows-specific Hadoop binaries (`winutils.exe`).

To use the workflow:

1. Push the project to a GitHub repository.
2. Ensure the workflow file exists in `.github/workflows/ci.yml`.
3. Create a pull request or push changes to trigger the pipeline.

For production environments, consider extending the workflow with:

- Code linting (Ruff or Flake8)
- Type checking (mypy)
- Code coverage reporting
- Python version matrix testing
- Automated deployment stages

Store secrets and credentials in GitHub Actions Secrets or a cloud identity solution rather than in source code.
