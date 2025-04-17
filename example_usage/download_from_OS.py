from eikondownloader.download import OSDownloader

# Initialize OSDownloader
downloader = OSDownloader(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="download",  # Path for files to upload
    secure=True,  # If "true" API requests will be secure (HTTPS), and insecure (HTTP) otherwise
    log_downloads=True,  # Log upload status
    log_files_path="log_files",  # Path for log files
    workers=5  # Use 5 concurrent workers
)

# Download files from MinIO
downloader.download_bucket(
    bucket_name="stock-dataset",  # Name of the minio bucket
    folder_prefix=None
)

# Print status after upload
print("Successfully downloaded files:", downloader.downloaded_files)
print("Download might be corrupted:", downloader.corrupted_files)
