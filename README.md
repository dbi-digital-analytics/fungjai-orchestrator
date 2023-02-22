# fungjai-orchestrator

Set up project fungjai-orchestrator สำหรับดึงข้อมูลจาก kafka ไปเก็บที่ minio แล้วส่งต่อไปยัง clickhouse โดยใช้ airflow เป็นตัวกลางในการส่งข้อมูล

สามารถไป clone project ได้ที่  [Github](https://github.com/dbi-digital-analytics/fungjai-orchestrator)
## Set up fungjai-orchestrator


ขั้นตอนก่อนเริ่ม run docker 

* สร้าง folder สำหรับใช้ต่อเป็นที่เก็บข้อมูลของ (airflow, minio, clickhouse, environment)
   ```shell
   mkdir dags logs plugins minio_data clickhouse_data .env
   ```
* Set environment ไว้ที่ไฟล์ .env
   ```shell
   #Airflow
   AIRFLOW_UID=50000

   # Minio
   MINIO_ROOT_USER=minio123
   MINIO_ROOT_PASSWORD=minio123
   ```

จากนั่นรันคำสั่งเพื่อ start project ขึ้นมา
```shell
docker-compose up -d
```
หรือ
```shell
make up
```
เราก็จะได้ docker container มาแล้วว :whale:

จากนั่นสร้าง 
* topic ที่ kafka ชื่อ first_kafka_topic
* bucket ที่ minio ชื่อ test-buckets
* table ที่ clickhouse ชื่อ test1

เพื่อรันคำสั่งตาม code เดิมได้เลย
