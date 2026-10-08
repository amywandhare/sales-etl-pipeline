from unittest.mock import Mock

import pytest


def test_pipeline_run_returns_gold_outputs(
    pipeline,
):
    outputs = pipeline.run()

    assert set(outputs.keys()) == {
        "sales",
        "customer",
        "data_quality",
    }

    assert outputs["sales"].count() > 0
    assert outputs["customer"].count() > 0
    assert outputs["data_quality"].count() > 0


def test_pipeline_paths_are_initialized(
    pipeline,
):
    assert pipeline.base_path
    assert "bronze" in pipeline.bronze_path
    assert "silver" in pipeline.silver_path
    assert "gold" in pipeline.gold_path


def test_pipeline_close_stops_spark(
    pipeline,
):
    pipeline.close()


def test_pipeline_run_raises_when_bronze_read_source_fails(
    pipeline,
):
    pipeline.bronze_pipeline.read_source = Mock(
        side_effect=ValueError("source failure")
    )

    with pytest.raises(
        ValueError,
        match="source failure",
    ):
        pipeline.run()


def test_pipeline_run_raises_when_silver_fails(
    pipeline,
):
    bronze_df = Mock()

    pipeline.bronze_pipeline.read_source = Mock(
        return_value=bronze_df
    )

    pipeline.bronze_pipeline.write = Mock()

    pipeline.bronze_pipeline.read = Mock(
        return_value=bronze_df
    )

    pipeline.silver_pipeline.run = Mock(
        side_effect=RuntimeError("silver failure")
    )

    with pytest.raises(
        RuntimeError,
        match="silver failure",
    ):
        pipeline.run()


def test_pipeline_run_raises_when_gold_fails(
    pipeline,
):
    bronze_df = Mock()
    silver_df = Mock()
    quality_df = Mock()

    pipeline.bronze_pipeline.read_source = Mock(
        return_value=bronze_df
    )

    pipeline.bronze_pipeline.write = Mock()

    pipeline.bronze_pipeline.read = Mock(
        return_value=bronze_df
    )

    pipeline.silver_pipeline.run = Mock(
        return_value=(silver_df, quality_df)
    )

    pipeline.gold_pipeline.run = Mock(
        side_effect=RuntimeError("gold failure")
    )

    with pytest.raises(
        RuntimeError,
        match="gold failure",
    ):
        pipeline.run()