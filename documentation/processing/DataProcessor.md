# `DataProcessor` Class

## Overview

The `DataProcessor` class is designed to handle data processing tasks for financial indices and exchange-traded products (ETPs). It provides methods to retrieve CSV files, extract unique RICs, and split lists into smaller chunks. The class is initialized with a base directory path and a processing mode, which determines the type of data being processed.

## Constructor

```python
DataProcessor(path: str, mode: str = "index")
```

### Parameters

- `path` : `str`
  - Base directory for data storage.
- `mode` : `str`, default=`"index"`
  - Processing mode, either `'index'` or `'etp'`.

### Raises

- `ValueError`
  - If the provided mode is neither `'index'` nor `'etp'`.

### Example

```python
processor = DataProcessor(path='/path/to/data', mode='index')
```

## Methods

### `get_csv_files`

```python
get_csv_files() -> list
```

#### Description

Retrieve all CSV files from the respective directory based on the processing mode.

#### Returns

- `list`
  - Sorted list of CSV file paths.

#### Raises

- `FileNotFoundError`
  - If no CSV files are found in the directory.

#### Example

```python
csv_files = processor.get_csv_files()
```

### `get_index_names`

```python
get_index_names(search_hidden: bool = False) -> list
```

#### Description

Retrieve all index names from the directory.

#### Parameters

- `search_hidden` : `bool`, default=`False`
  - Whether to include hidden directories.

#### Returns

- `list`
  - Sorted list of index names (folder names).

#### Example

```python
index_names = processor.get_index_names()
```

### `get_unique_rics`

```python
get_unique_rics(files: list, sep: str = ",") -> np.ndarray
```

#### Description

Extract and return unique RICs from a list of CSV files.

#### Parameters

- `files` : `list`
  - List of CSV file paths.
- `sep` : `str`, default=`,``
  - Separator for columns.

#### Returns

- `np.ndarray`
  - Sorted unique RICs as a NumPy array.

#### Example

```python
rics = processor.get_unique_rics(files=csv_files)
```

### `split_list`

```python
split_list(lst: Union[list, np.ndarray], chunk_size: int = 2000) -> list
```

#### Description

Splits the input list or NumPy array into smaller chunks of a specified size.

#### Parameters

- `lst` : `Union[list, np.ndarray]`
  - The input list or NumPy array to be split.
- `chunk_size` : `int`, default=`2000`
  - The size of each chunk.

#### Returns

- `list`
  - A list containing the chunks.

#### Example

```python
chunks = DataProcessor.split_list(lst=rics, chunk_size=1000)
```
