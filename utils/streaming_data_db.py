import os
import sys
from time import sleep
from pyarrow.parquet import ParquetFile
import pyarrow as pa 

from dotenv import load_dotenv
load_dotenv(".env")

from postgresql_client import PostgresSQLClient

TABLE_NAME = "iot.taxi_nyc_time_series"
PARQUET_FILE = "./data/2026/fhvhv_tripdata_2026-01.parquet"
NUM_ROWS = 10000

OFFSET_FILE = "utils/last_offset.txt"

def get_last_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_offset(current_row):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(current_row))

def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")
        return 

    last_offset = get_last_offset()
    print(f"--- Hệ thống khởi động lại từ offset: {last_offset} ---")

    pf = ParquetFile(PARQUET_FILE)
    
    batches_to_skip = last_offset // NUM_ROWS
    relative_offset = last_offset % NUM_ROWS
    
    batch_iter = pf.iter_batches(batch_size=NUM_ROWS)

    for _ in range(batches_to_skip):
        try:
            next(batch_iter)
        except StopIteration:
            print("Offset vượt quá số dòng trong file.")
            return

    current_global_idx = last_offset
    is_first_batch = True  

    for current_batch in batch_iter:
        df = pa.Table.from_batches([current_batch]).to_pandas()
        
        pickup_col = 'pickup_datetime' if 'pickup_datetime' in df.columns else 'tpep_pickup_datetime'
        dropoff_col = 'dropoff_datetime' if 'dropoff_datetime' in df.columns else 'tpep_dropoff_datetime'

        
        if is_first_batch:
            df_to_process = df.iloc[relative_offset:]
            is_first_batch = False
        else:
            df_to_process = df

        print(f"--- Đang nạp mẻ dữ liệu mới ({len(df_to_process)} dòng) ---")

        for _, row in df_to_process.iterrows():
            formatted_row = format_record(row, 3) 
            
            values_to_insert = [formatted_row.get(col.lower()) for col in columns]

            query = f"""
                INSERT INTO {TABLE_NAME} ({",".join(columns)})
                VALUES {tuple(values_to_insert)}
            """
            
            try:
                pc.execute_query(query)
                current_global_idx += 1
                save_offset(current_global_idx)
                print(f"Success [Dòng {current_global_idx}]: {formatted_row.get('tpep_pickup_datetime')}")
                
            except Exception as e:
                if "duplicate key" in str(e).lower():
                    current_global_idx += 1
                    save_offset(current_global_idx)
                    print(f" Skip [Dòng {current_global_idx}]: Đã tồn tại.")
                else:
                    print(f" Lỗi tại dòng {current_global_idx}: {e}")
                    return 
                    
            sleep(0.2)

    print(" Hoàn thành nạp toàn bộ file Parquet!")

def format_record(row, service_type):
    """

    service_type: 1 (Yellow), 2 (Green), 3 (HVFHV)

    """
    if service_type == 1:
        taxi_res = {
        'VendorID': row['VendorID'],
        'RatecodeID': row['RatecodeID'],
        'DOLocationID': row['DOLocationID'],
        'PULocationID': row['PULocationID'],
        'payment_type': row['payment_type'],
        'tpep_dropoff_datetime': str(row['tpep_dropoff_datetime']),
        'tpep_pickup_datetime': str(row['tpep_pickup_datetime']),
        'passenger_count': row['passenger_count'],
        'trip_distance': row['trip_distance'],
        'extra': row['extra'],
        'mta_tax': row['mta_tax'],
        'fare_amount': row['fare_amount'],
        'tip_amount': row['tip_amount'],
        'tolls_amount': row['tolls_amount'],
        'total_amount': row['total_amount'],
        'improvement_surcharge': row['improvement_surcharge'],
        'congestion_surcharge': row['congestion_surcharge'],
        'Airport_fee': row['Airport_fee']
    }
        return taxi_res

    elif service_type == 3:
        # Uber/Lyft không có mã Vendor ID số, ta map HV0003 (Uber) -> 3, HV0005 (Lyft) -> 5
        raw_vendor = str(row.get('hvfhs_license_num', ''))
        v_id = 3 if 'HV0003' in raw_vendor else (5 if 'HV0005' in raw_vendor else 0)
        return {
            'vendor_id': v_id,
            'service_type': 3,
            'tpep_pickup_datetime': str(row['pickup_datetime']),
            'tpep_dropoff_datetime': str(row['dropoff_datetime']),
            'passenger_count': float(row.get('passenger_count', 1.0)),
            'trip_distance': float(row.get('trip_miles', 0.0)),
            'rate_code_id': 1.0,
            'store_and_fwd_flag': 'N',
            'pulocationid': int(row['PULocationID']),
            'dolocationid': int(row['DOLocationID']),
            'payment_type': 1,
            'fare_amount': float(row.get('base_passenger_fare', 0.0)),
            'extra': float(row.get('tolls', 0.0)),
            'mta_tax': 0.0,
            'tip_amount': float(row.get('tips', 0.0)),
            'tolls_amount': float(row.get('tolls', 0.0)),
            'improvement_surcharge': 0.0,
            'total_amount': float(row.get('base_passenger_fare', 0.0)) + float(row.get('tips', 0.0)),
            'congestion_surcharge': float(row.get('congestion_surcharge', 0.0)),
            'airport_fee': 0.0
        }

if __name__ == "__main__":
    main()