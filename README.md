# DataFrame Chunker

Python generator for chunking Pandas DataFrames without splitting identical values across chunks.

## ⚡️ Features
* Guarantees that rows with the same value in a specified column remain in the same chunk.
* Each chunk will be >= `target_chunk_size`, unless it is the final remainder.
* Uses Python generator and `.iloc` slices to minimize RAM usage.
* Vectorized by NumPy boundary detection for $O(n)$ time complexity.
* Validates column existence and data sorting before execution.

## 📁 Structure
* `df_chunker.py`: Core logic/generator.
* `test_df_chunker.py`: Pytest suite (coverage for edge cases, sorting, and size constraints).

## 🪄 Usage
```python
from df_chunker import get_series_chunks


for chunk in get_series_chunks(df, series="dt", target_chunk_size=96):
    process(chunk)
```

## ✨ Testing
```bash
pip install -r requirements.txt
pytest test_df_chunker.py
```

## 🧮 Complexity
### Time Complexity: $O(n)$
The algorithm performs a single pass to check for sorting and a vectorized shift-and-compare to identify boundaries. Total execution time scales linearly with the number of rows.

### Space Complexity: $O(1)$
The generator yields `.iloc` slices, which are views of the original DataFrame rather than deep copies. Aside from a small boolean mask for boundary detection, memory overhead remains constant regardless of the total data size.