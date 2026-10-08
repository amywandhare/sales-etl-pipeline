from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

from sales_etl.utils.spark_session import (
    SparkSessionFactory,
)


@patch(
    "sales_etl.utils.spark_session.SparkSessionFactory._configure_windows_hadoop"
)
def test_create_calls_windows_configuration(
    mock_configure,
):
    factory = SparkSessionFactory(
        "test_app"
    )

    spark = factory.create()

    try:
        mock_configure.assert_called_once()

        assert isinstance(
            spark,
            SparkSession,
        )

        assert (
            spark.sparkContext.appName
            == "test_app"
        )

    finally:
        spark.stop()


@patch(
    "sales_etl.utils.spark_session.subprocess.run"
)
@patch(
    "sales_etl.utils.spark_session.os.name",
    "nt",
)
def test_windows_hadoop_validation_rejects_cmd_masquerade(
    mock_run,
):
    mock_process = Mock()

    mock_process.stdout = (
        "Microsoft Corporation"
    )

    mock_process.returncode = 0

    mock_run.return_value = (
        mock_process
    )

    with patch(
        "sales_etl.utils.spark_session.Path.exists",
        return_value=True,
    ):
        try:
            SparkSessionFactory._configure_windows_hadoop()
            assert False

        except RuntimeError:
            pass


@patch(
    "sales_etl.utils.spark_session.os.name",
    "nt",
)
def test_windows_hadoop_missing_winutils_raises():
    with patch(
        "sales_etl.utils.spark_session.Path.exists",
        return_value=False,
    ):
        try:
            SparkSessionFactory._configure_windows_hadoop()
            assert False

        except RuntimeError:
            pass


@patch(
    "sales_etl.utils.spark_session.subprocess.run"
)
@patch(
    "sales_etl.utils.spark_session.os.name",
    "nt",
)
def test_windows_hadoop_probe_failure_raises(
    mock_run,
):
    mock_run.side_effect = (
        OSError(
            "probe failed"
        )
    )

    with patch(
        "sales_etl.utils.spark_session.Path.exists",
        return_value=True,
    ):
        try:
            SparkSessionFactory._configure_windows_hadoop()
            assert False

        except RuntimeError:
            pass


@patch(
    "sales_etl.utils.spark_session.os.name",
    "posix",
)
def test_windows_hadoop_skipped_on_non_windows():
    SparkSessionFactory._configure_windows_hadoop()