import logging
import os

import gspread
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import Variable
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

MYSQL_CONN_ID = "mysql_default"
RAW_TABLE_NAME = "consumer_complaints_raw"

GOOGLE_CREDS_PATH_VAR = "google_service_account_json"
GOOGLE_SHEET_NAME_VAR = "consumer_financial_sheet_name"

TRANSFORMED_CSV_NAME = "consumer_complaints_transformed.csv"
DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def dump_to_mysql(**context) -> None:
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract_data_from_source", key="raw_file_path")

    df = pd.read_csv(raw_path)
    logger.info("Loaded %d raw rows from %s", len(df), raw_path)

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(RAW_TABLE_NAME, engine, if_exists="replace", index=False, chunksize=1000)
    logger.info("Wrote %d rows into MySQL table %s", len(df), RAW_TABLE_NAME)


def dump_to_googlesheet(**context) -> None:
    csv_path = os.path.join(DAGS_FOLDER, TRANSFORMED_CSV_NAME)
    df = pd.read_csv(csv_path)
    
    df = df.fillna("")
    logger.info("Loaded %d transformed rows from %s", len(df), csv_path)

    creds_path = Variable.get(GOOGLE_CREDS_PATH_VAR)
    sheet_name = Variable.get(GOOGLE_SHEET_NAME_VAR)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}"
    logger.info("Pushed data to %s", sheet_url)

    ti = context["ti"]
    ti.xcom_push(key="sheet_url", value=sheet_url)