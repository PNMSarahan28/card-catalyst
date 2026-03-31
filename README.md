# Card Catalyst – Transaction Processing Pipeline
## 📌 Overview
Card Catalyst is a PySpark-based data engineering pipeline designed to process financial transaction data.
It performs data cleaning, validation, masking of sensitive fields, summary generation, and storage into both local JSON files and a MySQL database.

## ⚙️ Features
Data Cleaning & Validation  
Removes duplicates, enforces required fields, and validates transaction amounts.

Data Masking  
Masks card numbers, hashes customer IDs, and generates transaction hashes for security.

Summaries

Daily merchant summaries

Risk metrics

Hourly transaction patterns

Data quality reports

Storage

Local JSON outputs (Linux filesystem)

MySQL tables for structured analytics

## 🖥️ Development Environment
This project is developed in a hybrid environment:

Windows Host

Running VSCode for development

Running MySQL for database storage

Ubuntu Guest (Virtual Machine)

Running PySpark and Hadoop

Handles data processing and writes outputs to Linux paths

This setup allows Spark to run natively in Linux while still leveraging Windows tools for coding and database management.

## 📂 File Paths
Local outputs are stored under:

Code
/home/prince-neezhel-sarahan/Documents/DE_Project1/data_files/
processed/ → Processed transactions

summaries/ → Summaries and reports

## 🚀 Running the Pipeline
Example command:

bash
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,mysql:mysql-connector-java:8.0.33 \
  --py-files src/utilities,src/extract,src/transform,src/load \
  src/main.py \
  --mode local --year 2025 --month 06 --day 24
## 📊 Output Verification
After execution:

Local JSON files are written to /home/prince-neezhel-sarahan/Documents/DE_Project1/data_files/...

MySQL tables updated:

processed_transactions

summary_daily_merchant_summary

summary_risk_metrics

summary_hourly_patterns

data_quality_reports

## 📝 Notes
Invalid transactions are only written if validation fails. In the sample run, all records were valid except one duplicate, so the invalid_transactions table remained empty.

Use .gitignore to exclude local data files and logs from GitHub:

Code
data_files/
pipeline.log
*.tmp
*.crc
