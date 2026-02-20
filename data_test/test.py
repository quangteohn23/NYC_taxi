import pandas as pd
import s3fs

# Khởi tạo kết nối tới MinIO Docker của bạn
s3_fs = s3fs.S3FileSystem(
    key='minio_access_key',
    secret='minio_secret_key',
    client_kwargs={'endpoint_url': 'http://localhost:9000'}
)

# Đường dẫn tới một file bất kỳ bạn đã transform thành công
# Cấu trúc: s3://bucket_name/folder_name/year/filename
path = "s3://processed/batch/2022/yellow_tripdata_2022-01.parquet"

# Đọc và in thử
df = pd.read_parquet(path, filesystem=s3_fs)
print("--- 5 Dòng đầu tiên của dữ liệu đã xử lý ---")
print(df.head(5))
# print("\n--- Thông tin cấu hình cột (Schema) ---")
# print(df.info())