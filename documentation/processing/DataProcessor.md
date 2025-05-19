# DataProcessor

The `DataProcessor` class is designed to handle data processing tasks, specifically for retrieving and processing CSV files from a specified directory. It supports two modes of operation: 'index' and 'etp', each with its own directory structure and column mappings.

## Constructor (__init__ method)

The `__init__` method initializes the `DataProcessor` with a specified path and mode.

### Parameters

- `path` (str): The directory path where the data files are located.
- `mode` (str, optional): The mode of operation, either 'index' or 'etp'. Default is 'index'.

### Raises

- `ValueError`: If the provided mode is not 'index' or 'etp'.

## Methods

### Public Methods

#### get_csv_files

Retrieves a sorted list of CSV files from the directory corresponding to the specified mode.

##### Returns

- A sorted list of CSV file paths found in the directory corresponding to the specified mode. (list)

##### Raises

- `KeyError`: If the mode is not found in `mode_mapping`.
- `OSError`: If an OS error occurs while searching for files.
- `FileNotFoundError`: If no CSV files are found in the specified directory.

##### Example Usage

```python
processor = DataProcessor(path='/data', mode='index')
csv_files = processor.get_csv_files()
print(csv_files)
```

#### get_index_names

Retrieves a sorted list of index names from the specified directory.

##### Parameters

- `search_hidden` (bool, optional): If True, includes hidden directories in the search. Default is False.

##### Returns

- A sorted list of index names found in the specified directory. (list)

##### Raises

- `FileNotFoundError`: If the provided path is not a valid directory.
- `OSError`: If an OS error occurs while searching for directories.
- `FileNotFoundError`: If no index names are found in the specified directory.

##### Example Usage

```python
processor = DataProcessor(path='/data', mode='index')
index_names = processor.get_index_names(search_hidden=True)
print(index_names)
```

#### get_unique_rics

Extracts and returns a sorted array of unique RICs from a list of CSV files.

##### Parameters

- `files` (list): A list of file paths to the CSV files containing RICs.
- `sep` (str, optional): The delimiter to use when reading the CSV files. Default is ",".

##### Returns

- A sorted NumPy array of unique RICs extracted from the provided CSV files. (np.ndarray)

##### Raises

- `KeyError`: If the mode is not found in `column_mapping`.
- `FileNotFoundError`: If no CSV files are provided.
- `FileNotFoundError`: If a file in the list is not found.
- `KeyError`: If the specified column is not found in a file.
- `pd.errors.EmptyDataError`: If a file is empty.
- `Exception`: For any other unexpected error while reading files.

##### Example Usage

```python
processor = DataProcessor(path='/data', mode='index')
csv_files = processor.get_csv_files()
unique_rics = processor.get_unique_rics(files=csv_files)
print(unique_rics)
```

#### split_list

Splits a list or a NumPy array into smaller chunks of a specified size.

##### Parameters

- `lst` (Union[list, np.ndarray]): The list or NumPy array to be split.
- `chunk_size` (int, optional): The size of each chunk. Default is 2000.

##### Returns

- A list of chunks, where each chunk is a sublist or subarray of the original input. (list)

##### Raises

- `ValueError`: If `chunk_size` is less than or equal to zero.
- `TypeError`: If the input is neither a list nor a NumPy array.

##### Example Usage

```python
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
chunks = DataProcessor.split_list(lst=data, chunk_size=3)
print(chunks)
```

### Hidden/Protected Methods

No hidden or protected methods are defined in the `DataProcessor` class.
