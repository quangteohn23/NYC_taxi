import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
from delta import *
dotenv.load_dotenv(".env")

from pyspark import SparkConf, SparkContext
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)

from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
DB_STAGING_TABLE = os.getenv("DB_STAGING_TABLE")

CFG_FILE = "./config/datalake.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME = datalake_cfg['bucket_name_2']

CFG_FILE_SPARK = "./config/spark.yaml"
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg["spark_config"]

DRIVER_MEMORY = spark_cfg.get('driver_memory', '2g')
EXECUTOR_MEMORY = spark_cfg.get('executor_memory', '2g')

def create_spark_session():
    # """
    # Sửa đổi: Chuyển sang dùng configure_spark_with_delta_pip và extra_packages
    # để tự động quản lý JDBC Driver và các thư viện AWS.
    # """
    from pyspark.sql import SparkSession

    try: 
        builder = SparkSession.builder \
            .appName("NYC Taxi Batch Processing to Staging") \
            .config("spark.driver.memory", DRIVER_MEMORY ) \
            .config("spark.executor.memory", EXECUTOR_MEMORY) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Thêm thư viện PostgreSQL JDBC vào extra_packages
        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=[
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "org.postgresql:postgresql:42.4.3"
            ]
        ).getOrCreate()
        
        logging.info('Spark session successfully created with JDBC and S3 support!')
        return spark

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session: {e}")
        return None

def process_dataframe(df, file_path):
    from pyspark.sql import functions as F
    
    # 1. Tự động nhận diện cột Pickup và Dropoff (vì Yellow và Green khác nhau)
    pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else \
                 ("lpep_pickup_datetime" if "lpep_pickup_datetime" in df.columns else "pickup_datetime")
    
    dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else \
                  ("lpep_dropoff_datetime" if "lpep_dropoff_datetime" in df.columns else "dropoff_datetime")

    # 2. Đồng nhất hóa tên cột (Rename) trước khi xử lý
    # Điều này giúp các file Yellow và Green trở nên giống hệt nhau
    df_standard = df.withColumnRenamed(pickup_col, "pickup_datetime") \
                    .withColumnRenamed(dropoff_col, "dropoff_datetime") \
                    .withColumnRenamed("vendorid", "vendor_id") \
                    .withColumnRenamed("VendorID", "vendor_id") \
                    .withColumnRenamed("ratecodeid", "rate_code_id") \
                    .withColumnRenamed("RatecodeID", "rate_code_id") \
                    .withColumnRenamed("pulocationid", "pickup_location_id") \
                    .withColumnRenamed("PULocationID", "pickup_location_id") \
                    .withColumnRenamed("dolocationid", "dropoff_location_id") \
                    .withColumnRenamed("DOLocationID", "dropoff_location_id") \
                    .withColumnRenamed("payment_type", "payment_type_id")

    # 3. Tạo các cột phái sinh (year, month, dow) và tọa độ giả
    df_enriched = df_standard.withColumn('year', F.year('pickup_datetime').cast("string")) \
                            .withColumn('month', F.date_format('pickup_datetime', 'MMMM')) \
                            .withColumn('dow', F.date_format('pickup_datetime', 'EEEE')) \
                            .withColumn('pickup_latitude', F.lit(0.0).cast("double")) \
                            .withColumn('pickup_longitude', F.lit(0.0).cast("double")) \
                            .withColumn('dropoff_latitude', F.lit(0.0).cast("double")) \
                            .withColumn('dropoff_longitude', F.lit(0.0).cast("double"))

    # 4. Gom nhóm (Aggregate)
    # Lưu ý: Ta gom nhóm để dữ liệu gọn nhẹ hơn trước khi đẩy vào Postgres (tốt cho máy 16GB)
    df_final = df_enriched.groupBy(
        'year', 'month', 'dow',
        'vendor_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id',
        'pickup_datetime', 'dropoff_datetime',
        'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude'
    ).agg(
        F.sum('passenger_count').alias('passenger_count'),
        F.sum('trip_distance').alias('trip_distance'),
        F.sum('extra').alias('extra'),
        F.sum('mta_tax').alias('mta_tax'),
        F.sum('fare_amount').alias('fare_amount'),
        F.sum('tip_amount').alias('tip_amount'),
        F.sum('tolls_amount').alias('tolls_amount'),
        F.sum('total_amount').alias('total_amount'),
        F.sum('improvement_surcharge').alias('improvement_surcharge'),
        F.sum('congestion_surcharge').alias('congestion_surcharge')
    )

    # 5. Gán loại dịch vụ (1: Yellow, 2: Green)
    service_type = 1 if 'yellow' in file_path.lower() else 2
    df_final = df_final.withColumn('service_type', F.lit(service_type))

    return df_final
def load_to_staging_table(df):
    """
    Save data after processing to Staging Area (PostgreSQL)
    """
    URL = f"jdbc:postgresql://{POSTGRES_HOST}:5434/{POSTGRES_DB}"
    
    # Ghi dữ liệu theo từng lô (batch) để giảm tải cho RAM và Database
    df.write \
        .format("jdbc") \
        .option("url", URL) \
        .option("dbtable", DB_STAGING_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "10000") \
        .mode("append") \
        .save()
    
    logging.info(f"Đã ghi dữ liệu thành công vào bảng {DB_STAGING_TABLE}")

if __name__ == "__main__":
    start_time = time.time()

    spark = create_spark_session()
    if not spark:
        sys.exit(1)

    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    # Xử lý tuần tự từng file để bảo vệ RAM 16GB
    parquet_files = client.list_parquet_files(BUCKET_NAME, prefix='batch/')
    
    for file in parquet_files:
        try:
            path = f"s3a://{BUCKET_NAME}/" + file
            logging.info(f"Đang xử lý file: {file}")

            df = spark.read.parquet(path)
            df_final = process_dataframe(df, file)
            
            # Load vào Staging
            load_to_staging_table(df_final)
            
            # Giải phóng bộ nhớ đệm sau mỗi file
            spark.catalog.clearCache()
            print("="*50)
            
        except Exception as e:
            logging.error(f"Lỗi khi xử lý file {file}: {e}")
            continue

    logging.info(f"Tổng thời gian xử lý: {time.time() - start_time:.2f} giây")
    spark.stop()