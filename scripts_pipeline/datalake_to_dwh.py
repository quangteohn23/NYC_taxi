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
    from pyspark.sql.types import DoubleType, IntegerType

    # 1. Chuyển tất cả tên cột về chữ thường
    df = df.toDF(*[c.lower() for c in df.columns])
    
    # 2. Xác định cột datetime chuẩn
    actual_pickup = "pickup_datetime" if "pickup_datetime" in df.columns else \
                    ("tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime")
    
    actual_dropoff = "dropoff_datetime" if "dropoff_datetime" in df.columns else \
                     ("tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime")

    # 3. Rename và ép kiểu cơ bản
    df_standard = df.withColumnRenamed(actual_pickup, "pickup_datetime") \
                    .withColumnRenamed(actual_dropoff, "dropoff_datetime") \
                    .withColumnRenamed("vendorid", "vendor_id") \
                    .withColumnRenamed("pulocationid", "pickup_location_id") \
                    .withColumnRenamed("dolocationid", "dropoff_location_id") \
                    .withColumnRenamed("payment_type", "payment_type_id") \
                    .withColumnRenamed("ratecodeid", "rate_code_id")

    # 4. Xử lý các cột số (Đảm bảo luôn tồn tại để không bị lỗi hàm SUM)
    numeric_cols = ['passenger_count', 'trip_distance', 'extra', 'mta_tax', 'fare_amount', 
                    'tip_amount', 'tolls_amount', 'total_amount', 'improvement_surcharge', 'congestion_surcharge']
    
    for c in numeric_cols:
        if c not in df_standard.columns:
            df_standard = df_standard.withColumn(c, F.lit(0.0).cast(DoubleType()))
        else:
            df_standard = df_standard.withColumn(c, F.col(c).cast(DoubleType()))

    # 5. Xử lý tọa độ (Nếu transform_data.py chưa tạo, ta tạo mặc định để không lỗi groupBy)
    geo_cols = ['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']
    for c in geo_cols:
        if c not in df_standard.columns:
            df_standard = df_standard.withColumn(c, F.lit(0.0).cast(DoubleType()))

    # 6. Gán service_type
    file_name = file_path.lower()
    service_type = 1 if 'yellow' in file_name else (2 if 'green' in file_name else 3)
    
    df_enriched = df_standard.withColumn('year', F.year('pickup_datetime').cast("string")) \
                             .withColumn('month', F.date_format('pickup_datetime', 'MMMM')) \
                             .withColumn('dow', F.date_format('pickup_datetime', 'EEEE')) \
                             .withColumn('service_type', F.lit(service_type).cast(IntegerType()))

    # 7. GroupBy và Aggregate
    # Lưu ý: Thêm 'service_type' vào groupBy để phân biệt Green/Yellow trong DB
    df_final = df_enriched.groupBy(
        'year', 'month', 'dow', 'service_type',
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
    
    return df_final
def load_to_staging_table(df):
    """
    Save data after processing to Staging Area (PostgreSQL)
    """
    URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    
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