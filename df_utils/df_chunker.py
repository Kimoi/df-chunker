import pandas as pd
import numpy as np


def get_series_chunks(df: pd.DataFrame, series: str, target_chunk_size: int):
    """
    Yields chunks of a DataFrame based on a specific series and size.

    Uses a generator to provide slices of the original DataFrame without duplicating data in memory.
    The same value in 'series' will never split across chunks.
    Each chunk will be >= target_chunk_size, unless it is the final remainder.
    """
    # Handle empty df input
    if df.empty:
        return
    
    # Series validation
    if series not in df.columns:
        raise ValueError(f"Column '{series}' not found")
    
    # Ensure data is grouped (sorted) to prevent data splitting
    if not (df[series].is_monotonic_increasing or df[series].is_monotonic_decreasing):
        raise ValueError(f"DataFrame must be sorted")
    
    # Boolean mask
    is_diff = df[series].ne(df[series].shift(-1))
    # Indices where the next row is a different date
    boundary_indices = np.where(is_diff)[0]

    start_idx = 0

    # Yield chunks
    for boundary in boundary_indices:
        current_chunk_size = (boundary + 1) - start_idx
        
        if current_chunk_size >= target_chunk_size:
            yield df.iloc[start_idx : boundary + 1]
            start_idx = boundary + 1

    # Yield the remaining data if any exists
    if start_idx < len(df):
        yield df.iloc[start_idx:]