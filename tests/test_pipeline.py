import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call

from src.ingestion.pipeline import run_pipeline


def make_chunk(rows):
    return pd.DataFrame({"col": list(range(rows))})


@patch("src.ingestion.pipeline.call_procedure")
@patch("src.ingestion.pipeline.load_data")
@patch("src.ingestion.pipeline.deduplicate")
@patch("src.ingestion.pipeline.clean_dataframe")
@patch("src.ingestion.pipeline.validate_dataframe")
@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
def test_pipeline_single_chunk(
    mock_get_engine,
    mock_read_file,
    mock_validate,
    mock_clean,
    mock_dedup,
    mock_load,
    mock_call_proc,
):
    engine = MagicMock()
    mock_get_engine.return_value = engine

    chunk = make_chunk(5)
    mock_read_file.return_value = [chunk]

    valid_df = make_chunk(4)
    reject_df = make_chunk(1)

    cleaned_df = valid_df.copy()
    deduped_df = make_chunk(3)

    mock_validate.return_value = (valid_df, reject_df)
    mock_clean.return_value = cleaned_df
    mock_dedup.return_value = deduped_df

    run_pipeline("test.csv")

    mock_validate.assert_called_once_with(chunk)
    mock_clean.assert_called_once_with(valid_df)
    mock_dedup.assert_called_once()
    mock_load.assert_called_once()

    args, kwargs = mock_load.call_args
    assert kwargs["valid_df"].equals(deduped_df)
    assert kwargs["reject_df"].equals(reject_df)

    mock_call_proc.assert_called_once_with(engine=engine)


@patch("src.ingestion.pipeline.call_procedure")
@patch("src.ingestion.pipeline.load_data")
@patch("src.ingestion.pipeline.deduplicate")
@patch("src.ingestion.pipeline.clean_dataframe")
@patch("src.ingestion.pipeline.validate_dataframe")
@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
def test_pipeline_multiple_chunks_aggregates_counts(
    mock_get_engine,
    mock_read_file,
    mock_validate,
    mock_clean,
    mock_dedup,
    mock_load,
    mock_call_proc,
):
    engine = MagicMock()
    mock_get_engine.return_value = engine

    chunk1 = make_chunk(3)
    chunk2 = make_chunk(2)

    mock_read_file.return_value = [chunk1, chunk2]

    mock_validate.side_effect = [
        (make_chunk(2), make_chunk(1)),
        (make_chunk(2), make_chunk(0)),
    ]

    mock_clean.side_effect = lambda df: df
    mock_dedup.side_effect = lambda df, viewed: df.iloc[:-1]

    run_pipeline("file.csv")

    assert mock_load.call_count == 2

    mock_call_proc.assert_called_once()

@patch("src.ingestion.pipeline.call_procedure")
@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
def test_pipeline_handles_no_chunks(
    mock_get_engine,
    mock_read_file,
    mock_call_proc,
):
    mock_get_engine.return_value = MagicMock()
    mock_read_file.return_value = []

    run_pipeline("empty.csv")

    mock_call_proc.assert_called_once()

@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
def test_pipeline_raises_and_logs_on_failure(
    mock_get_engine,
    mock_read_file,
):
    mock_get_engine.return_value = MagicMock()

    mock_read_file.side_effect = RuntimeError("Reader failure")

    with pytest.raises(RuntimeError):
        run_pipeline("bad.csv")

@patch("src.ingestion.pipeline.call_procedure")
@patch("src.ingestion.pipeline.load_data")
@patch("src.ingestion.pipeline.deduplicate")
@patch("src.ingestion.pipeline.clean_dataframe")
@patch("src.ingestion.pipeline.validate_dataframe")
@patch("src.ingestion.pipeline.read_file")
@patch("src.ingestion.pipeline.get_engine")
def test_pipeline_step_order(
    mock_get_engine,
    mock_read_file,
    mock_validate,
    mock_clean,
    mock_dedup,
    mock_load,
    mock_call_proc,
):
    engine = MagicMock()
    mock_get_engine.return_value = engine

    chunk = make_chunk(1)
    mock_read_file.return_value = [chunk]

    mock_validate.return_value = (chunk, pd.DataFrame())
    mock_clean.return_value = chunk
    mock_dedup.return_value = chunk

    call_sequence = []

    mock_validate.side_effect = lambda df: call_sequence.append("validate") or (df, pd.DataFrame())
    mock_clean.side_effect = lambda df: call_sequence.append("clean") or df
    mock_dedup.side_effect = lambda df, viewed: call_sequence.append("dedup") or df
    mock_load.side_effect = lambda **kwargs: call_sequence.append("load")

    run_pipeline("ordered.csv")

    assert call_sequence == ["validate", "clean", "dedup", "load"]
