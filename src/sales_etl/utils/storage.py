from __future__ import annotations

import posixpath
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from pyspark.sql import SparkSession


def _is_hadoop_uri(path: str) -> bool:
    return "://" in path


def join_storage_path(root: str | Path, *parts: str) -> str:
    """Join child names to local paths or Hadoop filesystem URIs."""
    root_text = str(root)
    parsed = urlsplit(root_text)
    if _is_hadoop_uri(root_text):
        joined_path = posixpath.join(parsed.path.rstrip("/"), *parts)
        return urlunsplit((parsed.scheme, parsed.netloc, joined_path, parsed.query, parsed.fragment))
    return str(Path(root_text).joinpath(*parts))


def storage_path_exists(spark: SparkSession, path: str | Path) -> bool:
    """Check paths through Hadoop FileSystem, including local file:// paths."""
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(str(path))
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(filesystem.exists(hadoop_path))


def metadata_path(path: str | Path) -> str:
    """Keep Hadoop URIs intact and normalize local paths to absolute paths."""
    path_text = str(path)
    if _is_hadoop_uri(path_text):
        return path_text.rstrip("/")
    return str(Path(path_text).resolve())