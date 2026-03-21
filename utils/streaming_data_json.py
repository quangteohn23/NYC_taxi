# mục đích: đọc dữ liệu taxi từ file .parquet
    # biến mỗi dòng thành JSON (giả lập streaming) => gửi từng dòng lên kafka
import os
import argparse # thư viện dùng để đọc tham số từ command line
import pandas as pd
import json
import logging
from time import sleep
from bson import json_util

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

logging.basicConfig(level=logging.INFO)
parser = argparse.ArgumentParser() 
parser.add_argument(
    "-m",
    "--mode",
    default = "setup",  # setup = chạy streaming, teardown: xóa topic kafka
    choices = ["setup", "teardown"],
    help = "Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events."
)

parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default = "localhost:9092",
    help = "Where the bootstrap server is"
)

args = parser.parse_args()

NUM_DEVICES = 1 # số topic
DATA_PATH = "data/2026/yellow_tripdata_2025-01.parquet"


def create_topic (admin, topic_name):
    """
        Create topic if not exists
    """
    try:
        topic = NewTopic(name = topic_name, num_partitions=1 , replication_factor=1)
        admin.create_topics([topic])
        logging.info(f"A new topic {topic_name} has been created!")
    except Exception:
        logging.info(f"Topic {topic_name} already exists. Skipping creation!")
        pass

def create_streams (servers):
    # kết nối kafka để đọc dữ liệu và gửi từng dòng lên topic
    """
        Create streaming data to Kafka Topic
    """
    producer = None
    admin = None
    # connect kafka
    for _ in range (10):
        try:
            producer = KafkaProducer(bootstrap_servers= servers)
            admin = KafkaAdminClient(bootstrap_servers = servers)
            break
        except Exception as e:
            logging.info(f"Trying to instantiate admin and producer with bosootstrap server {servers} with error {e}")
            sleep(10)
            pass
    
    if producer is None or admin is None:
        raise Exception("Cannot connect to Kafka")
    
    topic_name = "nyc_taxi_device"
    create_topic(admin, topic_name = topic_name)
    
    #send data
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"{DATA_PATH} not found")
    df = pd.read_parquet(DATA_PATH)
    
    for index, row in df.iterrows():
        data = format_record(row)
        producer.send(
            topic_name, json.dumps(data, default= json_util.default).encode("utf-8")
        )
        print(f"Sent: {data}")
        sleep(2)

    producer.flush()
    producer.close()
def format_record(row):
    # convert 1 dòng df thành json
    taxi_res = {}
    column_names = [
        'dolocationid', 'pulocationid', 'ratecodeid', 'vendorid',
        'congestion_surcharge', 'extra', 'fare_amount', 'improvement_surcharge',
        'mta_tax', 'passenger_count', 'payment_type', 'tip_amount',
        'tolls_amount', 'total_amount', 'dropoff_datetime', 'pickup_datetime',
        'trip_distance'
    ]
    for i, column_name in enumerate(column_names):
        value = row.get(column_name, None)
        if value is not None and 'datetime' in column_name:
            # vì kafka không serialize datetime => convert sang string
            taxi_res[column_name] = str(value)
        else:
            taxi_res[column_name] = value
    
    return taxi_res

def teardown_stream(topic_name, servers=["localhost:9092"]):
    # xoa topic kafka
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.delete_topics([topic_name])
        logging.info(f"Topic {topic_name} deleted")
    except Exception as e:
        logging.warning(f"Delete failed: {e}")

if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]
    
    logging.info("Tearing down all existing topics!")
    for _ in range (NUM_DEVICES):
        try:
            teardown_stream(f"nyc_taxi_device", [servers])
        except Exception as e:
            print(f"Topic nyc_taxi_device does not exist. Skipping...!")
    
    if mode == "setup":
        logging.info("Starting streaming...")
        create_streams([servers])