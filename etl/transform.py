import logging
import os

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)

MYSQL_CONN_ID = "mysql_default"
RAW_TABLE_NAME = "consumer_complaints_raw"

DROP_COLS = [
    "complaint_what_happened",
    "date_sent_to_company",
    "zip_code",
    "tags",
    "has_narrative",
    "consumer_consent_provided",
    "consumer_disputed",
    "company_public_response",
]

DIM_COLS = [
    "product",
    "sub_product",
    "issue",
    "sub_issue",
    "submitted_via",
    "company",
    "state",
    "timely",
    "company_response",
    "Month Year",
]

OUTPUT_CSV_NAME = "consumer_complaints_transformed.csv"
DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def transform_data(**context) -> None:
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = hook.get_pandas_df(f"SELECT * FROM {RAW_TABLE_NAME}")
    logger.info("Read %d raw rows from MySQL", len(df))

    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns])

    df["date_received"] = pd.to_datetime(df["date_received"], errors="coerce")
    df["Month Year"] = df["date_received"] + pd.offsets.MonthEnd(0)
    df = df.drop(columns=["date_received"])

    for col in DIM_COLS:
        if col != "Month Year":
            df[col] = df[col].fillna("N/A")

    grouped = (
        df.groupby(DIM_COLS, dropna=False)["complaint_id"]
        .nunique()
        .reset_index()
        .rename(columns={"complaint_id": "Count of complaint_id"})
    )

    output_path = os.path.join(DAGS_FOLDER, OUTPUT_CSV_NAME)
    grouped.to_csv(output_path, index=False)
    logger.info("Wrote %d summarized rows to %s", len(grouped), output_path)
