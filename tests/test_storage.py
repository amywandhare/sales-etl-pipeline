from pathlib import Path

from sales_etl.utils.storage import (
    _is_hadoop_uri,
    join_storage_path,
    metadata_path,
    storage_path_exists,
)


def test_join_preserves_cloud_filesystem_uris():
    assert (
        join_storage_path(
            "s3a://bucket/lake",
            "bronze",
            "raw_sales",
        )
        == "s3a://bucket/lake/bronze/raw_sales"
    )

    assert (
        join_storage_path(
            "abfss://container@account.dfs.core.windows.net/lake",
            "gold",
            "sales",
        )
        == "abfss://container@account.dfs.core.windows.net/lake/gold/sales"
    )

    assert (
        join_storage_path(
            "hdfs://namenode:8020/lake",
            "silver",
        )
        == "hdfs://namenode:8020/lake/silver"
    )


def test_join_handles_local_file_and_windows_paths():
    assert (
        join_storage_path(
            "file:///C:/lake",
            "bronze",
        )
        == "file:///C:/lake/bronze"
    )

    assert (
        join_storage_path(
            r"C:\lake",
            "bronze",
        )
        == str(
            Path(r"C:\lake")
            / "bronze"
        )
    )


def test_metadata_keeps_cloud_uri_without_local_resolution():
    assert (
        metadata_path(
            "s3a://bucket/lake/gold/sales"
        )
        == "s3a://bucket/lake/gold/sales"
    )


def test_metadata_resolves_local_path():
    result = metadata_path(
        "test_folder"
    )

    assert Path(result).is_absolute()


def test_is_hadoop_uri():
    assert _is_hadoop_uri(
        "s3a://bucket/lake"
    )

    assert _is_hadoop_uri(
        "abfss://container/lake"
    )

    assert not _is_hadoop_uri(
        r"C:\lake"
    )

    assert not _is_hadoop_uri(
        "local/folder"
    )


def test_storage_path_exists(
    spark_session,
    tmp_path,
):
    path = tmp_path / "exists"

    path.mkdir()

    assert storage_path_exists(
        spark_session,
        path,
    )


def test_storage_path_does_not_exist(
    spark_session,
    tmp_path,
):
    path = tmp_path / "missing"

    assert not storage_path_exists(
        spark_session,
        path,
    )