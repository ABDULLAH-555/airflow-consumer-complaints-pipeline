# 📊 Consumer Financial Complaints ETL Pipeline

![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow%202.x-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Container-Docker%20Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![MySQL](https://img.shields.io/badge/Database-MySQL%208.0-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Reporting-Google%20Sheets%20API-34A853?style=for-the-badge&logo=googlesheets&logoColor=white)

An enterprise-grade, containerized **Apache Airflow ETL Pipeline** that extracts consumer financial complaints from the **Consumer Financial Protection Bureau (CFPB) API** across all 50 US states, stages the raw records into MySQL, executes multi-dimensional data aggregation, pushes reporting metrics to Google Sheets, and sends automated stakeholder status reports via SMTP.

---

## 🏛️ Pipeline Architecture & Data Flow

```text
       +---------------------------------------------+
       |   CFPB Consumer Complaints RESTful API      |
       +---------------------------------------------+
                              |
                              v  (Task 1: extract.py with Exponential Backoff)
       +---------------------------------------------+
       |   Local Staging CSV: consumer_complaints_raw|
       +---------------------------------------------+
                              |
                              v  (Task 2: load.py via MySqlHook)
       +---------------------------------------------+
       |   MySQL Raw Table: consumer_complaints_raw  |
       +---------------------------------------------+
                              |
                              v  (Task 3: transform.py with Pandas Aggregation)
       +---------------------------------------------+
       |   Transformed Output: complaints_transformed|
       +---------------------------------------------+
                              |
                              v  (Task 4: FileSensor in 'reschedule' mode)
       +---------------------------------------------+
       |   File Verification Gating Check            |
       +---------------------------------------------+
                              |
                              v  (Task 5: load.py via gspread / OAuth2)
       +---------------------------------------------+
       |   Live Google Sheet Sync (Sanitized JSON)   |
       +---------------------------------------------+
                              |
                              v  (Task 6: EmailOperator via Jinja & SMTP)
       +---------------------------------------------+
       |   Stakeholder HTML Email Alert Delivered    |
       +---------------------------------------------+


⚙️ Core Technical Highlights
Resilient API Ingestion: Built-in exponential backoff retry mechanism (_get_with_retry) targeting HTTP 429 rate-limiting responses.

Deterministic Baseline: Employs a calibrated ANCHOR_DATE month-window to ensure identical, reproducible runs across environments.

Pipeline Idempotency: Implements full table replacement (if_exists="replace", chunksize=1000) in MySQL and pre-clearing in Google Sheets to guarantee zero data duplication upon re-runs.

Defensive Dimensional Aggregation: Pre-populates dimension nulls with "N/A" to prevent Pandas groupby from dropping complaints, and uses vectorized pd.offsets.MonthEnd for accurate time-series truncation.

Non-Blocking Resource Gating: Uses Airflow FileSensor configured with mode="reschedule" to release Celery worker execution slots while polling the filesystem.

Decoupled Architecture: Clean modular separation between orchestration (etl_dag.py), extraction (extract.py), transformation (transform.py), and multi-target loading (load.py).


📂 Project Structure

Airflow/
├── dags/
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py         # Task 1: API pagination, retries, and raw ingestion
│   │   ├── load.py            # Tasks 2 & 5: MySQL bulk loading & Google Sheets sync
│   │   └── transform.py       # Task 3: Column dropping, date offsets & groupby
│   ├── raw_data/              # Local scratch space for raw extraction drops
│   ├── etl_dag.py             # Main Airflow DAG orchestrator & task tree
│   └── google_key.json        # Google Cloud Service Account credentials (Excluded via .gitignore)
├── logs/                      # Airflow task execution runtime logs
├── plugins/                   # Custom plugins directory
├── docker-compose.yaml        # Airflow cluster (Webserver, Scheduler, Celery Worker, Redis, Postgres)
├── .env                       # Environment definitions & Airflow configurations
├── .gitignore                 # Sensitive secret & cache file exclusions
├── pyproject.toml             # Python package dependencies & metadata
└── README.md                  # Project architectural documentation


🚀 Setup & Execution Guide
1. Prerequisites
Docker Engine & Docker Compose installed and running.

Python 3.12+ (for local CLI development/linting).

A Google Cloud Service Account with Google Sheets and Google Drive API permissions.

2. Environment Configuration
Clone the repository:

Bash
git clone [https://github.com/ABDULLAH-555/Consumer-Financial-Complaints-Airflow-ETL.git](https://github.com/ABDULLAH-555/Consumer-Financial-Complaints-Airflow-ETL.git)
cd Consumer-Financial-Complaints-Airflow-ETL
Create a .env file in the project root:

Code snippet
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.0
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
Place your Google Service Account key inside the dags/ folder as google_key.json.

3. Service Initialization
Create the host logs directory and start the Docker containers:

PowerShell
New-Item -ItemType Directory -Force -Path .\logs
docker compose up -d
4. Airflow UI Setup (http://localhost:8080)
Login with username airflow and password airflow, then configure:

A. Airflow Variables (Admin ➔ Variables)
Key	Value	Description
google_service_account_json	/opt/airflow/dags/google_key.json	Absolute container path to credential key
consumer_financial_sheet_name	Consumer Complaints Report	Exact title of target Google Spreadsheet
B. Airflow Connections (Admin ➔ Connections)
MySQL Connection (mysql_default):

Conn Id: mysql_default

Conn Type: MySQL

Host / Port: Configured container/host MySQL endpoint

Login / Password: Database user credentials

SMTP Connection (smtp_default):

Conn Id: smtp_default

Conn Type: Email

Host: smtp.gmail.com

Port: 587

Login / Password: Configured application password

🧪 DAG Execution
Navigate to the Airflow UI dashboard.

Toggle the unpause switch for consumer_financial_etl.

Trigger the DAG manually or wait for the midnight schedule (0 0 * * *).

Monitor execution logs across all 6 sequential stages.

Verify the generated output directly on the target Google Sheet and check your inbox for the automated summary notification.