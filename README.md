## HealthTrends – Distributed Health Data Analyzer
## Project Overview
HealthTrends is an end-to-end distributed data engineering project that processes healthcare patient records using Kafka, HDFS, PySpark, and Python orchestration to generate daily health trend reports.

## Architecture
## Pipeline Flow:
Kafka Producer → Kafka Broker → Python Consumer → HDFS → PySpark → CSV Reports
Kafka streams patient health events

Python consumer validates records

HDFS stores data in date-partitioned format

PySpark aggregates healthcare trends

## Technologies Used
Apache Kafka (Producer & Consumer)

Python

HDFS (Cloudera QuickStart VM)

Apache Spark (PySpark)

## Workflow Summary
## 1. Kafka Ingestion
Patient health data is produced to Kafka topics.
## 2. Validation & Storage
Python consumer validates schema and stores clean records in HDFS:
/healthTrend/patient_json/date=YYYY-MM-DD/
## 3. Spark Processing
PySpark reads data from HDFS and computes:

Diagnosis-wise patient count

Average patient age

Gender ratio
## 4. Report Output
Final reports are written back to HDFS as CSV:
/healthTrend/reports/trend_summary/date=YYYY-MM-DD/
## 5. Orchestration
A Python orchestration script automates:

Data ingestion

HDFS operations

Spark job execution

Error handling and retries.
## 6. How to Run
python patient_data_pipeline.py 2025-12-22
## 7. Output
diagnosis,count,avg_age,gender_ratio
E11,2986,47.83,0.4973
J45,2938,47.63,0.5044
I25,2917,47.78,0.505
I10,2895,47.5,0.5105

## 📁 Repository Structure
healthtrend-pipeline/
healthtrend-pipeline/
├── kafka/              # Kafka producer & consumer scripts
├── spark/              # PySpark processing jobs
├── orchestrator/       # Python orchestration pipeline
├── docs/               # PRD, HLD, LLD documents
├── architecture.png    # System architecture diagram
└── README.md


##  Conclusion
This project demonstrates a real-world healthcare data pipeline using industry-standard big data technologies and showcases practical data engineering skills across ingestion, storage, processing, and reporting layers.
