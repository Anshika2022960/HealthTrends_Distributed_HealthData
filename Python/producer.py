from confluent_kafka import Producer
import json, time, random
from datetime import date, timedelta

conf = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "76F4XXEWK6NC4YXT",
    "sasl.password": "cfltztunpW5YwIOlKllPs+Ui24GD1IEK826ccgHJB3ppRR6Hgi6aDYqXQukmBGGQ",
}

producer = Producer(conf)
TOPIC = "HealthTrend_DataFlow"
start_day = date(2025, 12, 1)
d = start_day + timedelta(days=random.randint(0, 30))

def generate_patient_data():
    """Simulate one patient record."""
    return {

            "patient_id": "P%06d" % random.randint(1, 999999),
            "age": random.randint(1, 95),
            "gender": random.choice(["M", "F", "O"]),
            "diagnosis_code": random.choice(["E11", "I10", "J45", "I25"]),   # sample ICD-10 codes
            "visit_date": d.strftime("%Y-%m-%d"),
            "treatment_code": random.choice(["T100", "T200", "T300", "T400"]),
            "outcome": random.choice(["recovered", "ongoing", "deceased"])
        }
print("Sending real-time patient data to Kafka...")

try:
    while True:
        record = generate_patient_data()
        producer.produce(TOPIC, value=json.dumps(record).encode("utf-8"))
        producer.flush() 
        print("Sent:", record)
        time.sleep(1)              # 1 record per second -> "real time"
        
except KeyboardInterrupt:
    print(" Producer stopped.")