# OSUploader

## Overview

The `OSUploader` class is designed to upload files from a local directory to a MinIO bucket. It supports parallel uploads, checks for file integrity using MD5 hashes, and logs the upload process. The class can also delete local files after successful uploads.

## Constructor

```python
OSUploader(endpoint: str, access_key: str, secret_key: str, files_path: str, bucket_name: str = 'my-bucket', secure: bool = False, log_uploads: bool = True, log_files_path: str = "log_files_OSUploader", workers: int = 1)
```

### Parameters

- `endpoint` (str): The MinIO server endpoint.
- `access_key` (str): The access key for authentication.
- `secret_key` (str): The secret key for authentication.
- `files_path` (str): The local directory path containing files to upload.
- `bucket_name` (str, default='my-bucket'): The name of the MinIO bucket to upload files to.
- `secure` (bool, default=False): If True, use HTTPS to connect to the MinIO server.
- `log_uploads` (bool, default=True): If True, log the upload process.
- `log_files_path` (str, default="log_files_OSUploader"): The directory path to store log files.
- `workers` (int, default=1): The number of parallel workers for uploading files.

## Methods

### `_ensure_directory_exists`

```python
@staticmethod
_ensure_directory_exists(path: str) -> None
```

Ensures that a directory exists, creating it if necessary.

#### Parameters

- `path` (str): The directory path to check and create if it does not exist.

### `_ensure_bucket`

```python
_ensure_bucket() -> None
```

Ensures that the specified bucket exists, creating it if necessary.

### `calculate_md5`

```python
@staticmethod
calculate_md5(file_path: str) -> str
```

Computes the MD5 hash of a file.

#### Parameters

- `file_path` (str): The path to the file for which to compute the MD5 hash.

#### Returns

- `str`: The MD5 hash of the file.

### `_get_current_date`

```python
@staticmethod
_get_current_date() -> str
```

Gets the current system date, formatted as "DD-MMM-YYYY-HH-MM".

#### Returns

- `str`: The current date in the formatted datetime format.

### `upload_directory`

```python
upload_directory(remote_prefix: str = "", check_for_existence: bool = False) -> None
```

Recursively uploads a directory to MinIO with real-time logging.

#### Parameters

- `remote_prefix` (str, default=""): The prefix to prepend to the remote file paths.
- `check_for_existence` (bool, default=False): If True, check for existing files in the bucket and skip uploads if the file already exists and matches the hash.

### `_fetch_existing_files`

```python
_fetch_existing_files(remote_prefix: str) -> dict
```

Fetches existing files in the bucket with their ETags.

#### Parameters

- `remote_prefix` (str): The prefix to filter files in the bucket.

#### Returns

- `dict`: A dictionary of existing files with their ETags.

### `_upload_file`

```python
_upload_file(local_file_path: str, remote_path: str, check_for_existence: bool, existing_files: dict) -> bool
```

Uploads a single file with immediate logging.

#### Parameters

- `local_file_path` (str): The local file path to upload.
- `remote_path` (str): The remote file path in the bucket.
- `check_for_existence` (bool): If True, check for existing files in the bucket and skip uploads if the file already exists and matches the hash.
- `existing_files` (dict): A dictionary of existing files with their ETags.

#### Returns

- `bool`: True if the file was uploaded successfully, False otherwise.

### `delete_local_files`

```python
delete_local_files() -> None
```

Deletes local files after successful upload verification.

### `_write_log_file`

```python
_write_log_file(filename: str, data: list) -> None
```

Writes a list of file links to a log file, ensuring each entry is on a new line.

#### Parameters

- `filename` (str): The path to the log file.
- `data` (list): The list of file paths to log.
