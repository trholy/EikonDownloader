# EikonDownloader

This Python package is designed to download financial data from the Eikon API, upload files to a MinIO server, and process the data for analysis. It provides a comprehensive set of tools for handling financial data, including downloading, uploading, and processing.

## Features

* **EikonDownloader**: Download financial data from the Eikon API
* **OSDownloader**: Download files from a MinIO server
* **OSUploader**: Upload files to a MinIO server with parallel uploads and data integrity checks
* **DataProcessor**: Extract, convert, and filter data for further analysis

## Installation

You can install the package via pip:

```bash
pip install .
```

## Documentation

Read the documentation on GitLab Pages for more information on how to use the package.

## Usage

### Example Workflow

The package provides a comprehensive set of tools for handling financial data. Here's an example workflow:

1. Download financial data from the Eikon API using the `EikonDownloader` class.
2. Upload the downloaded data to a MinIO server using the `OSUploader` class.
3. Download files from the MinIO server using the `OSDownloader` class.
4. Process the downloaded data using the `DataProcessor` class.

### EikonDownloader: Download Financial Data from Eikon API

The `EikonDownloader` class allows you to download financial data from the Eikon API.

```python
from financial_data_downloader import EikonDownloader

# Initialize EikonDownloader
eikon_downloader = EikonDownloader(
    api_key="your-api-key",
    request_delay=1,
    request_limit_delay=3600,
    error_delay=5
)

# Download index chain data
eikon_downloader.get_index_chain(
    index_ric="your-index-ric",
    target_date="your-target-date",
    fields="your-fields",
    parameters=None,
    max_retries=10,
    pre_fix="0#."
)
```

### OSDownloader: Download Files from MinIO Server

The `OSDownloader` class allows you to download files from a MinIO server.

```python
from financial_data_downloader import OSDownloader

# Initialize OSDownloader
os_downloader = OSDownloader(
    endpoint="your-minio-server.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="your-files-path",
    secure=False,
    log_downloads=True,
    log_files_path="your-log-files-path",
    workers=1
)

# Download files from MinIO server
os_downloader.download_bucket(
    bucket_name="your-bucket-name",
    folder_prefix="your-folder-prefix"
)
```

### OSUploader: Upload Files to MinIO Server

The `OSUploader` class allows you to upload files to a MinIO server with parallel uploads and data integrity checks.

```python
from financial_data_downloader import OSUploader

# Initialize OSUploader
os_uploader = OSUploader(
    endpoint="your-minio-server.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="your-files-path",
    bucket_name="your-bucket-name",
    secure=False,
    log_uploads=True,
    log_files_path="your-log-files-path",
    workers=1
)

# Upload files to MinIO server
os_uploader.upload_directory(
    remote_prefix="your-remote-prefix",
    check_for_existence=False
)
```

### DataProcessor: Process Financial Data

The `DataProcessor` class allows you to extract, convert, and filter financial data for further analysis.

```python
from financial_data_downloader import DataProcessor

# Initialize DataProcessor
data_processor = DataProcessor(
    path="your-path",
    mode="index"
)

# Get CSV files
csv_files = data_processor.get_csv_files()

# Get unique RICs
rics = data_processor.get_unique_rics(
    files=csv_files,
    sep=","
)

# Split list into smaller chunks
chunks = data_processor.split_list(
    lst=rics,
    chunk_size=100
)
```

## Directory Structure

The package structure is as follows:

```
eikondownloader/
├── src/
│   ├── eikondownloader/
│   │   ├── __init__.py
│   │   ├── download/
│   │   │   ├── downloading.py
│   │   │   ├── __init__.py
│   │   ├── upload/
│   │   │   ├── uploader.py
│   │   │   ├── __init__.py
│   │   ├── processing/
│   │   │   ├── processor.py
│   │   │   ├── __init__.py
├── example_usage/
│   ├── download_constituents_stats.py
│   ├── index_chain_download.py
│   ├── download_index_stats.py
│   ├── time_series_download.py
├── pyproject.toml
├── README.md
├── LICENSE
├── .gitignore
```

## Dependencies

- `minio`: For uploading files to a MinIO server.
- `numpy`: For numerical computations and data manipulation.
- `eikon`: For accessing financial data from the Eikon API.
- `pandas`: For data processing and handling CSV files.

### Optional Development Dependencies
- `pytest`: For running tests.
- `ruff`: For linting Python code.

## License

This project is licensed under the **Creative Commons Attribution 4.0 International License (CC BY 4.0)**. You are free to share, adapt, and build upon the material, even for commercial purposes, as long as you provide appropriate credit. For more details, visit [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

## Attribution

When using or adapting this software, please give appropriate credit to the original author:  
Thomas R. Holy ([thomas.robert.holy@gmail.com](mailto:thomas.robert.holy@gmail.com)).


## Authors & Maintainers

- Thomas R. Holy, Ernst-Abbe-Hochschule Jena, (thomas.robert.holy@gmail.com)

## Contributing

If you’d like to contribute to the development of `dwdown`, feel free to fork the repository, create a branch for your feature or fix, and submit a pull request.

---
