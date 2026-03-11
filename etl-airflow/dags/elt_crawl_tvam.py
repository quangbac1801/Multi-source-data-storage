from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
import hashlib
import pandas as pd
import pyodbc


# ================== SQL SERVER CONNECTION ==================

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


# ================== CRAWL ==================
def get_last_page(report_type):
    url = f"https://www.tvam.vn/vi/quarter-report?report={report_type}&page=1"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    pages = soup.find_all("a", class_="page-link")
    nums = [int(p.text) for p in pages if p.text.isdigit()]
    return max(nums) if nums else 1


def crawl_report(report_type):
    news_list = []
    last_page = get_last_page(report_type)

    for page in range(1, last_page + 1):

        url = f"https://www.tvam.vn/vi/quarter-report?report={report_type}&page={page}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        ls_news = soup.find_all('div', class_='investor-relations-content-link-parent')

        for news in ls_news[:-1]:
            link_tag = news.find('a', class_='exportfile')
            if not link_tag:
                continue

            title = link_tag.text.strip()
            link = link_tag['href']
            time = news.find('div', class_='investor-relations-content-time')
            time = time.text.strip() if time else ""

            news_id = hashlib.md5(f"{title}{time}".encode()).hexdigest()

            news_list.append({
                "news_id": news_id,
                "report_type": report_type,
                "title": title,
                "date_publish": time,
                "link": f"https://www.tvam.vn/document/{report_type}/vi/{link}",
                "date_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

    return news_list


# ================== TASK 1 – Crawl + Load Staging ==================
def crawl_and_load_staging(**context):

    conn = get_connection("Staging_Finance")
    cursor = conn.cursor()

    all_reports = []
    for r in [10, 11, 12]:
        all_reports.extend(crawl_report(r))

    df_new = pd.DataFrame(all_reports)

    df_new.dropna(subset=["news_id", "title", "date_publish", "link"], inplace=True)
    df_new.drop_duplicates(subset=["news_id"], keep="first", inplace=True)

    df_old = pd.read_sql("SELECT * FROM stg_crawl_tvam", conn)

    new_records = df_new[~df_new["news_id"].isin(df_old["news_id"])]

    for _, row in new_records.iterrows():
        cursor.execute("""
            INSERT INTO stg_crawl_tvam (news_id, report_type, title, date_publish, link, date_update)
            VALUES (?, ?, ?, ?, ?, ?)
        """, row["news_id"], row["report_type"], row["title"], row["date_publish"], row["link"], row["date_update"])

    conn.commit()

    context['ti'].xcom_push(key="df_new", value=df_new.to_json())


# ================== TASK 2 – Sync Atomic ==================
def sync_staging_to_atomic():

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn_stg = get_connection("Staging_Finance")
    df_new = pd.read_sql("SELECT * FROM stg_crawl_tvam", conn_stg)
    conn_stg.close()

    if df_new.empty:
        return

    conn = get_connection("Atomic_Finance")
    cursor = conn.cursor()

    df_old = pd.read_sql("""
        SELECT *
        FROM atm_crawl_tvam
        WHERE is_current = 1 AND is_deleted = 0
    """, conn)

    # INSERT
    new_items = df_new[~df_new.news_id.isin(df_old.news_id)]

    for _, r in new_items.iterrows():
        cursor.execute("""
            INSERT INTO atm_crawl_tvam
            (news_id, report_type, title, date_publish, link)
            VALUES (?, ?, ?, ?, ?)
        """, r.news_id, r.report_type, r.title, r.date_publish, r.link)

    # UPDATE
    merged = df_new.merge(df_old, on="news_id", suffixes=("_new", "_old"))

    changed = merged[
        (merged.title_new != merged.title_old) |
        (merged.link_new != merged.link_old) |
        (merged.date_publish_new != merged.date_publish_old)
    ]

    for _, r in changed.iterrows():
        cursor.execute("""
            UPDATE atm_crawl_tvam
            SET is_current = 0,
                valid_to = ?
            WHERE news_id = ?
              AND is_current = 1
              AND is_deleted = 0
        """, now, r.news_id)

        # Insert
        cursor.execute("""
            INSERT INTO atm_crawl_tvam
            (news_id, report_type, title, date_publish, link)
            VALUES (?, ?, ?, ?, ?)
        """, r.news_id, r.report_type_new, r.title_new,
             r.date_publish_new, r.link_new)


    removed = df_old[~df_old.news_id.isin(df_new.news_id)]

    for nid in removed.news_id:
        cursor.execute("""
            UPDATE atm_crawl_tvam
            SET is_current = 0,
                is_deleted = 1,
                valid_to = ?
            WHERE news_id = ?
              AND is_current = 1
              AND is_deleted = 0
        """, now, nid)

    conn.commit()
    conn.close()



# ================== DAG ==================
default_args = {
    "owner": "bac",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "tvam_crawl_daily",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="30 17 * * *",
    catchup=False,
    tags=["tvam", "daily"]
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
