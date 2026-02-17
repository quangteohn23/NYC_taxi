import os
from minio import Minio
from minio.error import S3Error

class MinIOClient:
    def __init__(self, endpoint_url, access_key, secret_key, secure=False):
        """Khởi tạo kết nối duy nhất khi tạo object"""
        try:
            self.client = Minio(
                endpoint=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )
            # Kiểm tra thử kết nối 
            self.client.list_buckets()
            print("--- Kết nối MinIO thành công! ---")
        except Exception as e:
            print(f"--- Lỗi kết nối: {e} ---")
            self.client = None

    def create_bucket(self, bucket_name):
        """Tạo bucket nếu chưa tồn tại"""
        if not self.client: return
        
        try:
            found = self.client.bucket_exists(bucket_name)
            if not found:
                self.client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' đã được tạo.")
            else:
                print(f"Bucket '{bucket_name}' đã tồn tại.")
        except S3Error as e:
            print(f"Lỗi S3: {e}")

    def list_parquet_files(self, bucket_name, prefix=""):
        """Liệt kê các tệp .parquet một cách an toàn"""
        if not self.client: return []
        
        try:
            objects = self.client.list_objects(
                bucket_name, 
                prefix=prefix, 
                recursive=True
            )
            # Sử dụng generator để tiết kiệm bộ nhớ nếu danh sách tệp quá lớn
            return [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        except S3Error as e:
            print(f"Lỗi khi liệt kê tệp: {e}")
            return []

# --- Cách sử dụng ---
# client = MinIOClient("localhost:9000", "admin", "password123")
# files = client.list_parquet_files("my-data-bucket")
# print(files)