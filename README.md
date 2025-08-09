# E-Commerce Data Pipeline Project

This project is a cloud-native, real-time data pipeline for ingesting, processing, storing, monitoring, and analyzing e-commerce transaction data. It is designed for scalability, performance, and observability using AWS services, Apache Kafka, and the ELK stack. Dashboards and business intelligence are handled through Power BI Pro.

---

## 🚀 Project Overview

- **Real-time ingestion**: Synthetic e-commerce transaction events are generated and serialized in **Avro** format by a Python producer containerized on **ECS Fargate**, and streamed to **Kafka** hosted on an EC2 instance.
- **Data lake and processing**: Events are streamed to **Amazon S3**, cleaned and transformed using **PySpark** on **Amazon EMR**, and stored in **Parquet** format.
- **Data warehousing**: Transformed data is loaded into **Amazon Redshift Serverless** using the **S3 COPY command**.
- **Monitoring**: System metrics and logs are captured with **Metricbeat** (Kafka, Redshift) and **Filebeat** (Kafka logs), shipped to **Logstash**, and visualized via **Kibana** dashboards and **email alerts**.
- **Analytics & BI**: Final data is visualized using **Power BI Pro**, with interactive dashboards and What-If analysis features.

This pipeline enables near real-time operational insight into e-commerce transactions, providing robust analytics and proactive system monitoring for both technical and business stakeholders.

---

## 🧱 Architecture
<img src="Architecture/Architecture%20diagram.jpeg" alt="Architecture Diagram" width="500" height="300">

1. **Data Generation**: Python script generates synthetic e-commerce events every 5 seconds in Avro format and sends 10,000 records to Kafka.
2. **Kafka Broker**: Kafka is hosted on EC2, with a single-node setup configured with producers, topics, and a Kafka → S3 connector.
3. **Data Lake on S3**:
   - Raw Avro data lands in an S3 bucket under a `topics/` folder.
   - Cleaned and transformed data is written into `DWH/` folder in Parquet format by EMR.
4. **Batch Processing via EMR**:
   - PySpark jobs clean, validate, and transform incoming records.
   - Lambda triggers automate EMR job startup.
5. **Redshift Serverless**:
   - Receives clean Parquet data using the COPY command.
   - Schema follows a conformed star-flake model for optimized querying.
6. **Monitoring Stack**:
   - **Metricbeat**: Kafka (JMX module), Redshift (AWS CloudWatch module).
   - **Filebeat**: Kafka logs.
   - **Logstash**: Central log receiver that forwards to Elasticsearch.
   - **Kibana**: Visualizes logs, performance metrics, and threshold-based alerts.
7. **Business Intelligence**:
   - Power BI Pro dashboards for sales trends, KPIs, and What-If analytics.
   - Dashboards pull directly from Redshift and are designed for different personas (Ops, Analysts).

---

## ⚙️ Technologies Used

- **Cloud Infrastructure**: AWS (EC2, S3, Redshift Serverless, EMR, Lambda, ECS Fargate)
- **Data Streaming**: Apache Kafka (with JMX), Avro (schema-based serialization)
- **Data Processing**: Apache Spark (PySpark on EMR)
- **Data Storage**: Amazon S3 (raw, clean, rejected), Redshift Serverless
- **Monitoring & Logging**: Metricbeat, Filebeat, Logstash, Elasticsearch, Kibana
- **Automation**: AWS Lambda (folder cleanup, EMR orchestration)
- **Visualization**: Power BI Pro (interactive dashboards & What-If analysis)

---

## 📊 Schema & Storage

- **Star-Flake Schema**: The dimensional model consists of one fact table (e.g., sales events) and multiple dimension tables. Geography dimensions (Country → City → Shipping Address) are snowflaked to improve normalization and query performance.
- **S3 Layout**:
  - `s3://iti-ecommerce-all/topics/` → Incoming Avro records
  - `s3://iti-ecommerce-all/DWH/` → Validated and transformed Parquet data
  - `s3://iti-ecommerce-all/DWH/rejected/` → Failed or malformed records in Parquet



---

## 📣 Key Features

- Real-time ingestion of 10,000 events every 5 seconds
- Automated S3 cleanup with Lambda for raw and processed data
- EMR batch transformation and data cleansing
- Flexible and efficient Parquet-based storage
- Kafka log and metrics monitoring via ELK stack
- Kibana-based dashboards and alerting via email
- Business intelligence dashboards with Power BI Pro
- What-If slicers for simulation-based analytics

---

## 📁 Repository Structure

```bash
├── spark-jobs/
│   └── clean_transform.py
├── lambda/
│   └── s3_cleanup.py
├── kafka-config/
│   └── connector-config.json
├── dashboards/
│   └── powerbi.pbix
├── monitoring/
│   ├── metricbeat.yml
│   ├── filebeat.yml
│   └── logstash.conf
├── architecture/
│   └── diagrams.pdf/png
├── README.md
└── report/
    └── Final_Report.docx
```

---

## 🔒 Security Considerations

While this project prioritizes functional demonstration over strict security practices, the following best practices are encouraged for production:

- Use IAM roles with least privilege for S3, EMR, Lambda, Redshift
- Secure Kafka with SSL and authentication
- Apply security group restrictions and logging for EC2 instances

---

## 📬 Contact

This project was developed as part of the **ITI Intensive Training Program – Data Engineering Track**. For issues or inquiries, please open a GitHub issue or contact the team.

> *This project report is submitted as a prerequisite for completing the ITI Intensive Training Program.*

