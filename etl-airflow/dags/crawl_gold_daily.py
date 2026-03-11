from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import hashlib
import pyodbc


# =============== SQL SERVER CONNECTION ===============
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


# =============== UTILS ===============
def hash_id(s):
    return hashlib.md5(s.encode()).hexdigest()


def parse_number(text):
    return text.replace(",", "").strip() if text else None


# =============== CRAWL GOLD ===============
def crawl_gold(date):
    url = f"https://www.24h.com.vn/gia-vang-hom-nay-c425.html?ngaythang={date}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("table.gia-vang-search-data-table tbody")
    if not table:
        return []

    data = []
    for tr in table.select("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        loai_vang = tds[0].get_text(strip=True)
        gia_mua = parse_number(tds[1].find("span", class_="fixW").get_text())
        gia_ban = parse_number(tds[2].find("span", class_="fixW").get_text())
        gia_mua_hqua = parse_number(tds[3].get_text())
        gia_ban_hqua = parse_number(tds[4].get_text())

        row_id = hash_id(loai_vang + date)

        data.append({
            "id": row_id,
            "loai_vang": loai_vang,
            "gia_mua": gia_mua,
            "gia_ban": gia_ban,
            "gia_mua_homqua": gia_mua_hqua,
            "gia_ban_homqua": gia_ban_hqua,
            "ngay": date,
            "ngay_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return data


# ================= TASK 1 – Crawl + Clean + Staging =================
def crawl_and_load_staging(**context):
    today = datetime.now().strftime("%Y-%m-%d")
    raw_data = crawl_gold(today)

    df = pd.DataFrame(raw_data)

    # CLEAN
    df.dropna(inplace=True)
    df.drop_duplicates(subset=["id"], keep="first", inplace=True)

    conn = get_connection("Staging_Finance")
    cursor = conn.cursor()

    df_old = pd.read_sql("SELECT id FROM stg_crawl_gold", conn)

    new_records = df[~df["id"].isin(df_old["id"])]

    for _, r in new_records.iterrows():
        cursor.execute("""
            INSERT INTO stg_crawl_gold (id, loai_vang, gia_mua, gia_ban,
                                  gia_mua_homqua, gia_ban_homqua, ngay, ngay_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, r.id, r.loai_vang, r.gia_mua, r.gia_ban,
                       r.gia_mua_homqua, r.gia_ban_homqua, r.ngay, r.ngay_update)

    conn.commit()
    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="df_today", value=df.to_json())


# ================= TASK 2 – Sync Atomic =================
def sync_staging_to_atomic(**context):

    conn_stg = get_connection("Staging_Finance")
    df_new = pd.read_sql("SELECT * FROM stg_crawl_gold", conn_stg)
    conn_stg.close()

    conn = get_connection("Atomic_Finance")
    cursor = conn.cursor()

    df_old = pd.read_sql("SELECT * FROM atm_crawl_gold", conn)

    # NEW
    new_rows = df_new[~df_new.id.isin(df_old.id)]

    deleted_rows = df_old[~df_old.id.isin(df_new.id)]

    # UPDATED
    merged = df_new.merge(df_old, on="id", suffixes=("_new", "_old"))
    updated_rows = merged[
        (merged.loai_vang_new != merged.loai_vang_old) |
        (merged.gia_mua_new != merged.gia_mua_old) |
        (merged.gia_ban_new != merged.gia_ban_old)
    ]

    # DELETE
    for _, r in deleted_rows.iterrows():
        cursor.execute("DELETE FROM atm_crawl_gold WHERE id=?", r.id)

    # UPDATE
    for _, r in updated_rows.iterrows():
        cursor.execute("""
            UPDATE atm_crawl_gold
            SET loai_vang=?, gia_mua=?, gia_ban=?, gia_mua_homqua=?, gia_ban_homqua=?, ngay_update=?
            WHERE id=?
        """, r.loai_vang_new, r.gia_mua_new, r.gia_ban_new,
             r.gia_mua_homqua_new, r.gia_ban_homqua_new,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), r.id)

    # INSERT
    for _, r in new_rows.iterrows():
        cursor.execute("""
            INSERT INTO atm_crawl_gold (id, loai_vang, gia_mua, gia_ban,
                                  gia_mua_homqua, gia_ban_homqua, ngay, ngay_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, r.id, r.loai_vang, r.gia_mua, r.gia_ban,
             r.gia_mua_homqua, r.gia_ban_homqua, r.ngay, r.ngay_update)

    conn.commit()
    cursor.close()
    conn.close()


# ================= DAG =================
default_args = {
    "owner": "bac",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
        "gold_price_daily",
        default_args=default_args,
        start_date=datetime(2025, 12, 1),
        schedule_interval="30 17 * * *",
        catchup=False,
        tags=["gold", "daily"]
) as dag:
    t1 = PythonOperator(
        task_id="crawl_and_load_staging",
        python_callable=crawl_and_load_staging
    )

    t2 = PythonOperator(
        task_id="sync_staging_to_atomic",
        python_callable=sync_staging_to_atomic
    )

    t1 >> t2
