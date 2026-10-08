from __future__ import annotations

import os
import subprocess  # nosec B404

# Required to validate genuine Hadoop winutils.exe binaries.
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PYTHON_EXE = sys.executable
os.environ.setdefault("PYSPARK_PYTHON", PYTHON_EXE)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", PYTHON_EXE)


class SparkSessionFactory:
    """Create a consistent local Spark session for the ETL pipeline."""

    def __init__(self, app_name: str) -> None:
        self.app_name = app_name

    @staticmethod
    def _configure_windows_hadoop() -> None:
        if os.name != "nt":
            return

        hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop"))
        winutils_exe = hadoop_home / "bin" / "winutils.exe"
        if not winutils_exe.exists():
            raise RuntimeError(
                "PySpark on Windows requires a genuine Hadoop winutils.exe at "
                f"{winutils_exe}. Install compatible Hadoop Windows binaries and set HADOOP_HOME."
            )

        try:
            probe = subprocess.run(  # nosec B603
                [str(winutils_exe), "help"],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )   
            # Safe because:
            # - executable path is validated
            # - arguments are hardcoded
            # - shell=True is not used
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(
                f"Unable to validate Hadoop winutils.exe at {winutils_exe}."
            ) from exc

        if "microsoft corporation" in probe.stdout.casefold() or probe.returncode > 255:
            raise RuntimeError(
                f"{winutils_exe} is not a valid Hadoop winutils executable; "
                "it may be Windows cmd.exe renamed as winutils.exe. "
                "Install genuine Hadoop Windows binaries and set HADOOP_HOME."
            )

        os.environ["HADOOP_HOME"] = str(hadoop_home)

    def create(self) -> SparkSession:
        self._configure_windows_hadoop()
        return (
            SparkSession.builder.appName(self.app_name)
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.pyspark.python", PYTHON_EXE)
            .config("spark.pyspark.driver.python", PYTHON_EXE)
            .config("spark.executorEnv.PYSPARK_PYTHON", PYTHON_EXE)
            .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", PYTHON_EXE)
            .config("spark.hadoop.fs.permissions.enabled", "false")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.hadoop.fs.raw.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .getOrCreate()
        )