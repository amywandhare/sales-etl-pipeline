from pathlib import Path
from unittest.mock import Mock, patch

from sales_etl.cli import main


@patch("sales_etl.cli.SalesETLPipeline")
@patch(
    "sys.argv",
    [
        "sales-etl",
    ],
)
def test_cli_uses_default_arguments(
    mock_pipeline_class,
):
    pipeline_instance = Mock()

    mock_pipeline_class.return_value = (
        pipeline_instance
    )

    main()

    mock_pipeline_class.assert_called_once()

    pipeline_instance.run.assert_called_once()

    pipeline_instance.close.assert_called_once()


@patch("sales_etl.cli.SalesETLPipeline")
@patch(
    "sys.argv",
    [
        "sales-etl",
        "--input-path",
        "custom.csv",
        "--base-path",
        "custom_lake",
        "--log-dir",
        "logs",
        "--app-name",
        "custom_app",
    ],
)
def test_cli_uses_custom_arguments(
    mock_pipeline_class,
):
    pipeline_instance = Mock()

    mock_pipeline_class.return_value = (
        pipeline_instance
    )

    main()

    mock_pipeline_class.assert_called_once_with(
        base_path="custom_lake",
        app_name="custom_app",
        input_path="custom.csv",
        log_dir=Path("logs"),
    )

    pipeline_instance.run.assert_called_once()

    pipeline_instance.close.assert_called_once()


@patch("sales_etl.cli.SalesETLPipeline")
@patch(
    "sys.argv",
    [
        "sales-etl",
    ],
)
def test_cli_closes_pipeline_on_failure(
    mock_pipeline_class,
):
    pipeline_instance = Mock()

    pipeline_instance.run.side_effect = (
        RuntimeError(
            "pipeline failed"
        )
    )

    mock_pipeline_class.return_value = (
        pipeline_instance
    )

    try:
        main()
    except RuntimeError:
        pass

    pipeline_instance.close.assert_called_once()