from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import hashlib
import pyodbc


def get_conn(database):
    return pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=host.docker.internal,1435;"
        f"Database={database};"
        "UID=airflow_user;"
        "PWD=AirflowStrong123!;"
        "Encrypt=no;TrustServerCertificate=yes;"
    )


# TASK 1 – LOAD INTO STAGING

def extract_to_staging(**context):

    url = 'https://v6.exchangerate-api.com/v6/7bc1014f61d4ad8b6f3a5f4b/latest/USD'
    data = requests.get(url).json()

    rows = []

    date_key = datetime.strptime(
        data["time_last_update_utc"], "%a, %d %b %Y %H:%M:%S %z"
    ).strftime("%Y-%m-%d")

    etl_date = datetime.now().strftime("%Y-%m-%d")

    for currency, rate in data["conversion_rates"].items():

        raw_id = f"{data['base_code']}_{currency}_{date_key}"
        hashed_id = hashlib.md5(raw_id.encode()).hexdigest()

        rows.append({
            "id": hashed_id,
            "date_key": date_key,
            "etl_date": etl_date,
            "time_last_update_utc": data["time_last_update_utc"],
            "time_next_update_utc": data["time_next_update_utc"],
            "base_code": data["base_code"],
            "currency": currency,
            "rate": rate
        })

    df_new = pd.DataFrame(rows)
    df_new.dropna(inplace=True)
    df_new.drop_duplicates(subset=["id"], inplace=True)

    conn = get_conn("Staging_Finance")
    cursor = conn.cursor()

    inserted_rows = 0

    for _, row in df_new.iterrows():

        cursor.execute("SELECT COUNT(*) FROM stg_api_exchange_rate WHERE id=?", row["id"])
        if cursor.fetchone()[0] > 0:
            continue  # skip duplicate

        cursor.execute("""
            INSERT INTO stg_api_exchange_rate
            (id, date_key, etl_date, time_last_update_utc, time_next_update_utc,
             base_code, currency, rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row["id"], row["date_key"], row["etl_date"],
        row["time_last_update_utc"], row["time_next_update_utc"],
        row["base_code"], row["currency"], row["rate"])

        inserted_rows += 1

    conn.commit()
    print(f"[STAGING] Inserted {inserted_rows} new rows.")

    context["ti"].xcom_push(key="df_new", value=df_new.to_json())


def load_to_atomic(**context):

    # Lấy dữ liệu từ STAGING
    conn_stg = get_conn("Staging_Finance")
    df_new = pd.read_sql("SELECT * FROM stg_api_exchange_rate", conn_stg)

    # Kết nối ATOMIC
    conn = get_conn("Atomic_Finance")
    cursor = conn.cursor()

    inserted = 0

    for _, row in df_new.iterrows():

        cursor.execute("SELECT COUNT(*) FROM atm_api_exchange_rate WHERE id=?", row["id"])
        if cursor.fetchone()[0] > 0:
            continue

        cursor.execute("""
            INSERT INTO atm_api_exchange_rate
            (id, date_key, etl_date, time_last_update_utc, time_next_update_utc,
             base_code, currency, rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row["id"], row["date_key"], row["etl_date"],
        row["time_last_update_utc"], row["time_next_update_utc"],
        row["base_code"], row["currency"], row["rate"])

        inserted += 1

    conn.commit()
    print(f"[ATOMIC] Inserted {inserted} rows.")


#  DAG CONFIG

default_args = {
    "owner": "bac",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="exchange_rate_daily_sql",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["sqlserver", "etl", "exchange_rate"]
) as dag:

    t1 = PythonOperator(
        task_id="extract_to_staging",
        python_callable=extract_to_staging
    )

    t2 = PythonOperator(
        task_id="load_to_atomic",
        python_callable=load_to_atomic
    )

    t1 >> t2
