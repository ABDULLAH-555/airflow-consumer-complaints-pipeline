import logging
import os
import time
from datetime import date
import pandas as pd
import requests
logger = logging.getLogger(__name__)

API_URL = (
    "https://www.consumerfinance.gov/data-research/consumer-complaints/"
    "search/api/v1/"
)
STATES_URL = (
    "https://gist.githubusercontent.com/mshafrir/2646763/raw/"
    "8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json"
)

ANCHOR_DATE = date(2024, 8, 31)
WINDOW_DAYS = 30
MAX_RECORDS_PER_STATE = 50
PAGE_SIZE = 100 
SLEEP_BETWEEN_CALLS = 1.0  
MAX_RETRIES = 3

RAW_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw_data"
)


def get_all_states() -> list[str]:
    resp = requests.get(STATES_URL, timeout=30)
    resp.raise_for_status()
    return list(resp.json().keys())


def _get_with_retry(params: dict) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(API_URL, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            wait = 2 ** attempt
            logger.warning("Rate limited, backing off %ss", wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {params}")


def fetch_state_complaints(state: str, min_date: str, max_date: str) -> list[dict]:
    records: list[dict] = []
    frm = 0
    while len(records) < MAX_RECORDS_PER_STATE:
        page_size = min(PAGE_SIZE, MAX_RECORDS_PER_STATE - len(records))
        params = {
            "size": page_size,
            "frm": frm,
            "date_received_min": min_date,
            "date_received_max": max_date,
            "state": state,
        }
        data = _get_with_retry(params)
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        records.extend(h["_source"] for h in hits)
        frm += page_size
        time.sleep(SLEEP_BETWEEN_CALLS)
        if len(hits) < page_size:
            break 
    return records


def extract_all(**context) -> None:
    min_date = (ANCHOR_DATE.replace(day=1)).isoformat()
    max_date = ANCHOR_DATE.isoformat()

    states = get_all_states()
    logger.info("Extracting complaints for %d states", len(states))

    all_records: list[dict] = []
    for state in states:
        try:
            state_records = fetch_state_complaints(state, min_date, max_date)
            logger.info("%s: %d records", state, len(state_records))
            all_records.extend(state_records)
        except Exception:
            logger.exception("Failed to fetch state %s, skipping", state)
        time.sleep(SLEEP_BETWEEN_CALLS)

    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    raw_path = os.path.join(RAW_DATA_DIR, "consumer_complaints_raw.csv")
    pd.DataFrame(all_records).to_csv(raw_path, index=False)
    logger.info("Wrote %d rows to %s", len(all_records), raw_path)

    ti = context["ti"]
    ti.xcom_push(key="raw_file_path", value=raw_path)
