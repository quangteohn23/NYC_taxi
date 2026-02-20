import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

from pyspark import SparkConf, SparkContext
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

CFG_FILE = "./config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]
YEARS_TO_PROCESS = ["2022", "2023", "2024"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
BUCKET_NAME_3 = datalake_cfg['bucket_name_3']

def load_to_delta (endpoint_url, access_key, secret_key):

    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip


    builder = SparkSession.builder \
        .appName("Converting to Delta Lake") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 

    spark = configure_spark_with_delta_pip(
    builder, 
    extra_packages=[
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "io.delta:delta-spark_2.12:3.1.0"
    ]
).getOrCreate()
    logging.info('Spark session successfully created!')

    client_wrapper = MinIOClient(endpoint_url, access_key, secret_key)
    client_wrapper.create_bucket(BUCKET_NAME_3)

    path_read_root = f"s3a://{BUCKET_NAME_2}/{datalake_cfg['folder_name']}/*/*.parquet"
    path_write_delta = f"s3a://{BUCKET_NAME_3}/{datalake_cfg['folder_name']}/nyc_taxi_delta"

    for y in YEARS_TO_PROCESS:
        try:
            # Đường dẫn đọc dữ liệu theo từng năm
            path_read = f"s3a://{BUCKET_NAME_2}/{datalake_cfg['folder_name']}/{y}/*.parquet"
            
            logging.info(f"--- Bắt đầu xử lý năm {y} ---")
            df = spark.read.parquet(path_read)
            
            # Năm đầu tiên dùng overwrite để tạo bảng mới, các năm sau dùng append
            write_mode = "overwrite" if y == YEARS_TO_PROCESS[0] else "append"
            
            logging.info(f"Đang ghi năm {y} vào Delta với mode: {write_mode}")
            df.write \
                .format("delta") \
                .mode(write_mode) \
                .option("mergeSchema", "true") \
                .save(path_write_delta)
                  
            logging.info(f"Hoàn tất xử lý năm {y}")
            
            # Giải phóng cache sau mỗi năm
            spark.catalog.clearCache()
            
        except Exception as e:
            logging.error(f"Lỗi khi xử lý năm {y}: {e}")
            traceback.print_exc()
    
    logging.info("--- TẤT CẢ CÁC NĂM ĐÃ ĐƯỢC LOAD VÀO DELTA LAKE ---")
    spark.stop()
    
if __name__ == "__main__":
    load_to_delta (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)