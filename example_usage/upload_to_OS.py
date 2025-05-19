from eikondownloader.upload import OSUploader


# Initialize OSUploader
uploader = OSUploader(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    files_path="download",  # Path for files to upload
    bucket_name="stock-dataset",  # Name of the minio bucket
    secure=True,  # If "true" API requests will be secure (HTTPS), and insecure (HTTP) otherwise
    log_uploads=True,  # Log upload status
    log_files_path="log_files",  # Path for log files
    workers=5  # Use 5 concurrent workers
)

delete_files = False

# Upload files to MinIO
uploader.upload_directory(check_for_existence=True)

if delete_files:
    uploader.delete_local_files()

# Print status after upload
print("Successfully uploaded files:", uploader.uploaded_files)
print("Upload might be corrupted:", uploader.corrupted_files)
