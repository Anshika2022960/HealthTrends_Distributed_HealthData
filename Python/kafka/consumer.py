from confluent_kafka import Consumer
import json
import requests
import time
from datetime import datetime
import pandas as pd
import os
import csv


# Output folder (local Windows)
OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)

fields = ["patient_id", "age", "gender", "diagnosis_code", "visit_date", "treatment_code", "outcome"]
BATCH_SIZE = 500



conf = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "76F4XXEWK6NC4YXT",
    "sasl.password": "cfltztunpW5YwIOlKllPs+Ui24GD1IEK826ccgHJB3ppRR6Hgi6aDYqXQukmBGGQ",
    "group.id": "healthtrends-consumer-group",
    "auto.offset.reset": "earliest", 
    "enable.auto.commit": True
}

TOPIC = "HealthTrend_DataFlow"
c = Consumer(conf)
c.subscribe([TOPIC])

def new_csv_writer():
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = os.path.join(OUT_DIR, f"patient_data_{ts}.csv")
    f = open(csv_path, "w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    return csv_path, f, w

print("✅ Consumer started... Press Ctrl+C to stop")

batch_count = 0
file_path, f, w = new_csv_writer()
print("Writing to:", file_path)



try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Kafka error:", msg.error())
            continue

        rec = json.loads(msg.value().decode("utf-8"))
        row = {k: rec.get(k, "") for k in fields}
        w.writerow(row)
        batch_count += 1

        if batch_count % 100 == 0:
            print("✅ stored", batch_count, "messages in current file")

        if batch_count >= BATCH_SIZE:
            f.close()
            print("🎉 Batch file completed:", file_path)

            batch_count = 0
            file_path, f, w = new_csv_writer()
            print("Writing to:", file_path)

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    try:
        f.close()
    except:
        pass
    c.close()
    print("✅ Consumer stopped.")


#---------------- HDFS CONFIG--------------------------
# HDFS_IP = "192.168.1.37"
# NAMENODE_PORT = "50070"
# HDFS_DIR = "/healthTrend/patient_data/"
# BATCH_SIZE = 10

# def write_to_hdfs_csv(batch):
#     """Write a batch of records as CSV into HDFS (date-partitioned)."""
#     if not batch:
#         print("   (empty batch, nothing to write)")
#         return

#     df = pd.DataFrame(batch)
#     csv_data = df.to_csv(index=False)

#     today = datetime.utcnow().strftime("%Y-%m-%d")
#     filename = f"patient_{int(time.time())}.csv"
#     hdfs_path = f"/healtTrend/patient_data/date={today}/{filename}"

#     url = (
#         f"http://{HDFS_IP}:{NAMENODE_PORT}/webhdfs/v1"
#         f"{hdfs_path}?op=CREATE&overwrite=true"
#     )
#     print(f"📁 Uploading CSV to HDFS: {hdfs_path}")

#     try:
#         # Step 1 – get redirect URL
#         step1 = requests.put(url, allow_redirects=False, timeout=10)
#         if "Location" not in step1.headers:
#             print("❌ No redirect URL from HDFS:", step1.text)
#             return

#         upload_url = step1.headers["Location"]

#         # Step 2 – upload CSV
#         step2 = requests.put(upload_url, data=csv_data, timeout=30)

#         if step2.status_code == 201:
#             print("✅ CSV stored in HDFS successfully\n")
#         else:
#             print("❌ Failed to write CSV:", step2.status_code, step2.text)
#     except Exception as e:
#         print("HDFS error:", repr(e))

# batch = []
# try:
#     while True:
#         msg = consumer.poll(1.0)

#         if msg is None:
#             print("  (no message yet...)")
#             continue

#         if msg.error():
#             print("Consumer error:", msg.error())
#             continue

#         # Step 1: decode JSON from Kafka
#         try:
#             data = json.loads(msg.value().decode("utf-8"))
#         except Exception as e:
#             print("JSON decode error:", e)
#             continue

#         print("📥 Raw Kafka record:", data)

#         # Step 4: ADD TO BATCH
#         if len(batch) >= BATCH_SIZE:
#             write_to_hdfs_csv(batch)
#             batch = []  # clear batch



# except KeyboardInterrupt:
#     print("Stopped consumer.")
# finally:
#     write_to_hdfs_csv(batch)
#     consumer.close()

