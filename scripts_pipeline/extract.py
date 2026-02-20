import sys
import os
from glob import glob
from minio import Minio

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'..', 'utils'))
if utils_path not in sys.path:
    sys.path.append(utils_path)


from helpers import load_cfg
from minio_utils import MinIOClient

CFG_FILE = "./config/datalake.yaml"
YEARS = ["2022", "2023","2024"]

def extract_load(endpoint_url, access_key, secret_key):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    target_bucket = datalake_cfg["bucket_name_1"]
    
    minio_wrapper = MinIOClient (
        endpoint_url = endpoint_url,
        access_key = access_key,
        secret_key = secret_key
    )
    if not minio_wrapper.client:
        print("Không thể kết nối tới MinIO. Vui lòng kiểm tra cấu hình.")
        return
    minio_wrapper.create_bucket(target_bucket)

    for year in YEARS:
        print(f"\n--- Đang xử lý dữ liệu năm: {year} ---")

        # Tìm tất cả file .parquet của năm đó ở máy cục bộ
        local_folder = os.path.join(nyc_data_cfg["folder_path"], year)
        all_fps = glob(os.path.join(local_folder, "*.parquet"))

        for fp in all_fps:
            file_name = os.path.basename(fp)

            object_name = os.path.join(datalake_cfg["folder_name"],year, file_name)
            
            try:
                print(f"Đang tải lên: {file_name} -> {object_name}")
                
                minio_wrapper.client.fput_object(
                    bucket_name=target_bucket,
                    object_name=object_name,
                    file_path=fp,
                )
            except Exception as e:
                print(f"Lỗi khi upload file {fp}: {e}")

if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    # Lấy thông tin từ file config
    extract_load(
        endpoint_url=datalake_cfg['endpoint'],
        access_key=datalake_cfg['access_key'],
        secret_key=datalake_cfg['secret_key']
    )