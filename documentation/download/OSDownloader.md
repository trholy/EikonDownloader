# OSDownloader

## Overview

The `OSDownloader` class is designed to facilitate the downloading of files from a MinIO server. It supports downloading entire buckets or specific folders, verifying file integrity using MD5 checksums, and logging the download process.

## Constructor

```python
OSDownloader(endpoint: str, access_key: str, secret_key: str, files_path: str, secure: bool = False, log_downloads: bool = True, log_files_path: str = "log_files_OSDownloader", workers: int = 1)
```

### Parameters

- `endpoint` (str): The MinIO server endpoint.
- `access_key` (str): The access key for authentication.
- `secret_key` (str): The secret key for authentication.
- `files_path` (str): The local directory where files will be downloaded.
- `secure` (bool, default=False): If True, the connection to the MinIO server will be secure (HTTPS).
- `log_downloads` (bool, default=True): If True, download logs will be generated.
- `log_files_path` (str, default="log_files_OSDownloader"): The directory where log files will be stored.
- `workers` (int, default=1): The number of worker threads to use for downloading files.

## Methods

### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(path: str) -> None
```

Ensures that a directory exists, creating it if necessary.

#### Parameters

- `path` (str): The path of the directory to check.

### `_ensure_bucket`

```python
_ensure_bucket(bucket_name: str) -> None
```

Checks if a bucket exists in the MinIO server and logs the result.

#### Parameters

- `bucket_name` (str): The name of the bucket to check.

### `calculate_md5`

```python
@staticmethod
calculate_md5(file_path: str) -> str
```

Computes the MD5 hash of a file.

#### Parameters

- `file_path` (str): The path to the file.

#### Returns

- `str`: The MD5 hash of the file.

### `_get_current_date`

```python
@staticmethod
_get_current_date() -> str
```

Gets the current system date formatted as "DD-MMM-YYYY-HH-MM".

#### Returns

- `str`: The formatted date string.

### `_get_remote_files`

```python
_get_remote_files(bucket_name: str, folder_prefix: str) -> list
```

Retrieves a list of remote files from a specified bucket and folder prefix.

#### Parameters

- `bucket_name` (str): The name of the bucket.
- `folder_prefix` (str): The prefix of the folder to list files from.

#### Returns

- `list`: A list of remote files.

### `download_bucket`

```python
download_bucket(bucket_name: str, folder_prefix: Optional[str] = None) -> None
```

Recursively downloads a bucket or folder from MinIO, verifying file integrity and logging the process.

#### Parameters

- `bucket_name` (str): The name of the bucket to download.
- `folder_prefix` (str, optional): The prefix of the folder to download. If not specified, the entire bucket is downloaded.

### `_download_file`

```python
_download_file(bucket_name: str, local_file_path: str, remote_path: str) -> bool
```

Downloads a single file and verifies its integrity.

#### Parameters

- `bucket_name` (str): The name of the bucket.
- `local_file_path` (str): The local path where the file will be saved.
- `remote_path` (str): The remote path of the file to download.

#### Returns

- `bool`: True if the file was downloaded and verified successfully, False otherwise.

### `_verify_file_integrity`

```python
_verify_file_integrity(bucket_name: str, local_file_path: str, remote_path: str) -> bool
```

Verifies if the local file matches the remote file's checksum.

#### Parameters

- `bucket_name` (str): The name of the bucket.
- `local_file_path` (str): The local path of the file.
- `remote_path` (str): The remote path of the file.

#### Returns

- `bool`: True if the file integrity is verified, False otherwise.

### `_write_log_file`

```python
_write_log_file(filename: str, data: list) -> None
```

Writes a list of file links to a log file, ensuring each entry is on a new line.

#### Parameters

- `filename` (str): The path of the log file.
- `data` (list): The list of file links to write to the log file.
