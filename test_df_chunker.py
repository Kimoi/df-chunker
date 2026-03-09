import pandas as pd
import numpy as np
import pytest
from df_chunker import get_series_chunks


@pytest.fixture
def sample_df():
    """Fixture to provide the standard test data"""
    dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:00:05", freq="s")
    example = np.concatenate([
        dfs[1:2].repeat(2),
        dfs[2:3].repeat(3),
        dfs[3:4].repeat(1)
    ])
    return pd.DataFrame({"dt": example})

def test_chunk_size_small(sample_df):
    """Case 1: Chunk size 1-2 (Expected 3 chunks)"""
    result = list(get_series_chunks(sample_df, "dt", 2))
    assert len(result) == 3
    assert len(result[0]) == 2
    assert len(result[1]) == 3
    assert len(result[2]) == 1

def test_chunk_size_medium(sample_df):
    """Case 2: Chunk size 3-5 (Expected 2 chunks)"""
    result = list(get_series_chunks(sample_df, "dt", 3))
    assert len(result) == 2
    assert len(result[0]) == 5

def test_chunk_size_large(sample_df):
    """Case 3: Chunk size 6+ (Expected 1 chunk)"""
    result = list(get_series_chunks(sample_df, "dt", 6))
    assert len(result) == 1
    assert len(result[0]) == 6

def test_empty_dataframe():
    """Case 4: Empty DataFrame (Expected 0 chunks)"""
    df_empty = pd.DataFrame(columns=["dt"])
    result = list(get_series_chunks(df_empty, "dt", 1))
    assert len(result) == 0

def test_invalid_column(sample_df):
    """Case 5: Invalid Column Name should raise ValueError"""
    with pytest.raises(ValueError, match="Column 'wrong' not found"):
        list(get_series_chunks(sample_df, "wrong", 1))

def test_unsorted_data():
    """Case 6: Unsorted data should raise ValueError to prevent data fragmentation"""
    df_unsorted = pd.DataFrame({"dt": ["A", "B", "A"]})
    with pytest.raises(ValueError, match="DataFrame must be sorted"):
        list(get_series_chunks(df_unsorted, "dt", 1))