# Lab 2 - Spam Email Analytics Pipeline

## Overview
End-to-end data analytics pipeline for spam email detection using Snowflake, Airflow, dbt, and Preset.

## Dataset
- **Source:** Email Spam Detection Dataset (Kaggle)
- **Size:** 5,572 emails labeled as spam or ham

## Architecture
Raw CSV → Airflow ETL → Snowflake → dbt ELT → Preset Dashboard

## Components

### Airflow DAGs
- `spam_email_etl.py` — Loads raw email CSV into Snowflake RAW_SPAM_EMAILS table
- `dbt_dag.py` — Runs dbt models, tests, and snapshots daily

### dbt Models
- `stg_spam_emails` — Staging view with derived features (message length, word count, spam flag)
- `spam_summary` — Abstract table with spam/ham statistics per label

### dbt Tests
- unique, not_null on EMAIL_ID
- not_null on LABEL and IS_SPAM
- accepted_values on LABEL (ham, spam)

### Preset Dashboard
- Pie chart: Spam vs Ham distribution
- Bar chart: Average message length by label

## Results
- Ham: 4,825 emails (86.6%)
- Spam: 747 emails (13.4%)
