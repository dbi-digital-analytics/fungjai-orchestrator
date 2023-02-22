from minio import Minio
from confluent_kafka import Consumer
import pandas as pd
import os
from datetime import datetime
import time

def upload_to_minio(data_list):
    # local config
    PATH = "C:/Users/Few/Desktop/Code_Test/python_test/kafka_to_minio"

    # minio config
    ACCESS_KEY = "minio123"
    SECRET_KEY = "minio123"
    MINIO_API_HOST = "localhost:9000"
    BUCKET_NAME = "test-buckets"
    MINIO_CLIENT = Minio(
        MINIO_API_HOST, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )

    # time
    str_date_time = datetime.now().strftime("%d-%m-%Y")
    count_time = int(round(time.time() * 1000))

    # file name
    FILE_NAME = f"mood_{str_date_time}_{count_time}.csv"

    # convert List to DataFrame
    df = pd.DataFrame({"mood": data_list})
    df.to_csv(f"{PATH}/{FILE_NAME}", index=False)

    # logging file name
    print(f"{PATH}/{FILE_NAME}")

    # upload to minio
    try:
        MINIO_CLIENT.fput_object(BUCKET_NAME, FILE_NAME, f"{PATH}/{FILE_NAME}")
        print("Success upload to bucket")
        os.remove(f"{PATH}/{FILE_NAME}")
    except:
        print("Error upload to bucket")


# kafka config
c = Consumer(
    {
        "bootstrap.servers": "localhost:9092",  # replace with your Kafka bootstrap servers
        "group.id": "my-consumer-group",  # replace with your consumer group ID
        "auto.offset.reset": "earliest",
    }
)
c.subscribe(["first_kafka_topic"])

# pull data from kafka
data_list = []
count = 0
while count < 3:
    message = c.poll(1.0)  # pull messages from kafka every 1 second
    if message is None:
        print("message is None")
        count += 1
    else:
        text = message.value().decode("utf-8")
        print(f"Received message: {text}")

        data_list.append(text)

        # upload_to_minio(data_list)

        data_list.clear()
