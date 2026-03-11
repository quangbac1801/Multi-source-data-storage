from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import hashlib
import time
import pyodbc

# ================= SQL SERVER CONNECTION =================
def get_connection(db_name):
    return pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=host.docker.internal\\BAC,1435;"
        f"Database={db_name};"
        "UID=airflow_user;"
        "PWD=AirflowStrong123!;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

# ================= CONFIG =================
URL = "https://vn.investing.com/rates-bonds/world-government-bonds?maturity_from=10&maturity_to=310"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "vi-VN,vi;q=0.9"
}

# ================= UTILS =================
def make_hash_id(country, name, tenor, time_str):
    raw = f"{country}|{name}|{tenor}|{time_str}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

# ================= CRAWL =================
def crawl_bond():
    import cloudscraper
    from bs4 import BeautifulSoup

    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"}
    )

    r = scraper.get(
        "https://www.investing.com/rates-bonds/world-government-bonds",
        timeout=30
    )

    if r.status_code != 200:
        raise Exception(f"Blocked: {r.status_code}")

    soup = BeautifulSoup(r.text, "lxml")
    tables = soup.select("table[id^='rates_bonds_table_']")
    rows = []

    etl_ts = int(time.time())

    for table in tables:
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 9:
                continue

            country = tds[0].span.get("title") if tds[0].span else None
            name = tds[1].get_text(strip=True)
            tenor = name.split()[-1]
            time_str = tds[8].get_text(strip=True)

            rid = hashlib.md5(
                f"{country}|{name}|{tenor}|{time_str}".encode()
            ).hexdigest()

            rows.append({
                "id": rid,
                "country": country,
                "name": name,
                "tenor": tenor,
                "yield_value": float(tds[2].text.replace(",", "")),
                "previous": float(tds[3].text.replace(",", "")),
                "high": float(tds[4].text.replace(",", "")),
                "low": float(tds[5].text.replace(",", "")),
                "change": float(tds[6].text.replace("+", "").replace(",", "")),
                "change_pct": tds[7].text,
                "time": time_str,
                "etl_timestamp": etl_ts
            })

    return pd.DataFrame(rows)



# ================= TASK 1 – STAGING =================
def crawl_and_load_staging():
    df = crawl_bond()

    df.dropna(inplace=True)
    df.drop_duplicates(subset=["id"], inplace=True)

    conn = get_connection("Staging_Finance")
    cursor = conn.cursor()

    df_old = pd.read_sql("SELECT id FROM stg_crawl_bond", conn)
    new_rows = df[~df.id.isin(df_old.id)]

    for _, r in new_rows.iterrows():
        cursor.execute("""
            INSERT INTO stg_crawl_bond
            (id, country, name, tenor, yield_value, previous, high, low,
             change, change_pct, time, etl_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        r.id, r.country, r.name, r.tenor, r.yield_value,
        r.previous, r.high, r.low,
        r.change, r.change_pct, r.time, r.etl_timestamp)

    conn.commit()
    conn.close()

# ================= TASK 2 – ATOMIC =================
def sync_staging_to_atomic():
    conn_stg = get_connection("Staging_Finance")
    df_new = pd.read_sql("SELECT * FROM stg_crawl_bond", conn_stg)
    conn_stg.close()

    conn = get_connection("Atomic_Finance")
    cursor = conn.cursor()
    df_old = pd.read_sql("SELECT * FROM atm_crawl_bond", conn)

    # INSERT NEW
    new_rows = df_new[~df_new.id.isin(df_old.id)]

    # UPDATE
    merged = df_new.merge(df_old, on="id", suffixes=("_new", "_old"))
    updated_rows = merged[
        (merged.yield_value_new != merged.yield_value_old) |
        (merged.change_new != merged.change_old)
    ]

    for _, r in updated_rows.iterrows():
        cursor.execute("""
            UPDATE atm_crawl_bond
            SET yield_value=?, previous=?, high=?, low=?,
                change=?, change_pct=?, etl_timestamp=?
            WHERE id=?
        """,
        r.yield_value_new, r.previous_new, r.high_new, r.low_new,
        r.change_new, r.change_pct_new, r.etl_timestamp_new, r.id)

    for _, r in new_rows.iterrows():
        cursor.execute("""
            INSERT INTO atm_crawl_bond
            (id, country, name, tenor, yield_value, previous, high, low,
             change, change_pct, time, etl_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        r.id, r.country, r.name, r.tenor, r.yield_value,
        r.previous, r.high, r.low,
        r.change, r.change_pct, r.time, r.etl_timestamp)

    conn.commit()
    conn.close()

# ================= DAG =================
default_args = {
    "owner": "bac",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="tpcp_crawl_daily",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["bond", "investing"]
) as dag:

    t1 = PythonOperator(
        task_id="crawl_and_load_staging_bond",
        python_callable=crawl_and_load_staging
    )

    t2 = PythonOperator(
        task_id="sync_staging_to_atomic_bond",
        python_callable=sync_staging_to_atomic
    )

    t1 >> t2
