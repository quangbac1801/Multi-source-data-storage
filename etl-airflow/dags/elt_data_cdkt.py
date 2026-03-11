from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import os
import io
import re
import math
import hashlib
import tempfile
from decimal import Decimal, InvalidOperation

import pandas as pd
import pyodbc

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ================= CONFIG =================
GDRIVE_SA_JSON = os.getenv("GDRIVE_SA_JSON", "/opt/airflow/secrets/airflow-drive-etl-key.json")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "171TR3KQSHc50Sz96gG4NxYSz_pGI1fp2")

REPORT_TYPE = "Cân đối kế toán"

SQL_SERVER = os.getenv("SQL_SERVER", r"host.docker.internal\BAC,1435")
SQL_UID = os.getenv("SQL_UID", "airflow_user")
SQL_PWD = os.getenv("SQL_PWD", "AirflowStrong123!")


# ================= DB =================
def get_connection(db: str):
    return pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={SQL_SERVER};"
        f"Database={db};"
        f"UID={SQL_UID};"
        f"PWD={SQL_PWD};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def parse_vn_number(x):

    if x is None or pd.isna(x):
        return None

    s = str(x).strip()
    if s == "":
        return None

    s = s.replace(" ", "").replace(".", "").replace(",", "")

    if not re.fullmatch(r"-?\d+", s):
        return None

    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def sql_safe_number(x):

    if x is None:
        return None

    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        if abs(v) > 1e15:
            return None
        return v
    except Exception:
        return None


def is_null_or_zero(x) -> bool:
    if x is None:
        return True
    try:
        return Decimal(x) == 0
    except Exception:
        try:
            return float(x) == 0.0
        except Exception:
            return True


def extract_period_from_filename(filename: str):

    m = re.search(r"(\d{8})", filename)
    return int(m.group(1)) if m else None


def find_balance_sheet_header_row(df: pd.DataFrame):

    df_norm = df.fillna("").astype(str).applymap(lambda x: x.lower())
    for i in range(min(80, len(df_norm))):
        row_txt = df_norm.iloc[i].to_string()
        if ("chỉ tiêu" in row_txt) and ("mã số" in row_txt) and ("số cuối kỳ" in row_txt):
            return i
    return None


# ================= GOOGLE DRIVE =================
def drive_service():
    creds = service_account.Credentials.from_service_account_file(
        GDRIVE_SA_JSON,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def list_excel_files(service):

    q = (
        f"'{GDRIVE_FOLDER_ID}' in parents and trashed=false and "
        "("
        "mimeType='application/vnd.ms-excel' or "
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        ")"
    )
    res = service.files().list(
        q=q,
        fields="files(id,name,mimeType,modifiedTime,md5Checksum)"
    ).execute()
    return res.get("files", [])


def download_file(service, file_id: str, out_path: str):
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


# ================= PARSE EXCEL =================
def parse_balance_sheet_rows(file_path: str, source_name: str):

    period = extract_period_from_filename(source_name)
    if not period:
        return []

    xls = pd.ExcelFile(file_path)
    rows = []
    now = datetime.now()

    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, header=None)
        header_row = find_balance_sheet_header_row(df)
        if header_row is None:
            continue

        # lấy 4 cột đầu
        data = df.iloc[header_row + 1:, :4].copy()
        data.columns = ["account_name", "code", "end_raw", "begin_raw"]

        for _, r in data.iterrows():
            code = str(r["code"]).strip()
            if not re.fullmatch(r"\d+", code):
                continue

            account_name = None
            if not pd.isna(r["account_name"]):
                account_name = str(r["account_name"]).strip()
                if account_name == "":
                    account_name = None

            end_val = parse_vn_number(r["end_raw"])
            begin_val = parse_vn_number(r["begin_raw"])


            if is_null_or_zero(end_val) and is_null_or_zero(begin_val):
                continue

            unique_id = md5(f"{source_name}|{code}|{period}")

            rows.append({
                "unique_id": unique_id,
                "source_name": source_name,
                "account_name": account_name,
                "code": code,
                "end_of_period": end_val,
                "begin_of_period": begin_val,
                "report_type": REPORT_TYPE,
                "period": period,
                "created_date": now,
            })

    return rows


def ensure_helper_tables():

    conn = get_connection("landing_finance")
    cur = conn.cursor()
    cur.execute("""
    IF OBJECT_ID('dbo.etl_file_log', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.etl_file_log (
        file_id        NVARCHAR(100) NOT NULL PRIMARY KEY,
        file_name      NVARCHAR(255) NOT NULL,
        modified_time  DATETIME2     NULL,
        md5_checksum   NVARCHAR(64)  NULL,
        processed_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
      );
    END
    """)
    conn.commit()
    cur.close()
    conn.close()


# ================= TASK 0: FILTER FILE =================
def filter_new_drive_files(**context):
    service = drive_service()
    files = list_excel_files(service)

    conn = get_connection("landing_finance")
    df_log = pd.read_sql("SELECT file_id FROM dbo.etl_file_log", conn)
    conn.close()

    processed = set(df_log["file_id"].astype(str)) if not df_log.empty else set()
    new_files = [f for f in files if str(f["id"]) not in processed]

    context["ti"].xcom_push(key="new_files", value=new_files)


# ================= TASK 1: DRIVE -> LANDING =================
def drive_to_landing(**context):
    files = context["ti"].xcom_pull(
        key="new_files",
        task_ids="filter_new_drive_files"
    )
    if not files:
        return

    service = drive_service()

    conn = get_connection("landing_finance")
    cur = conn.cursor()

    for f in files:
        file_id = str(f["id"])
        file_name = f["name"]
        mime = f.get("mimeType", "")
        ext = ".xlsx" if "spreadsheetml.sheet" in mime else ".xls"
        tmp_path = os.path.join(tempfile.gettempdir(), f"{file_id}{ext}")

        download_file(service, file_id, tmp_path)

        rows = parse_balance_sheet_rows(tmp_path, file_name)


        for r in rows:
            cur.execute("""
                IF NOT EXISTS (SELECT 1 FROM dbo.Can_doi_ke_toan WHERE unique_id=?)
                BEGIN
                    INSERT INTO dbo.Can_doi_ke_toan
                    (
                        unique_id,
                        source_name,
                        account_name,
                        code,
                        end_of_period,
                        begin_of_period,
                        report_type,
                        period,
                        created_date
                    )
                    VALUES (?,?,?,?,?,?,?,?,?)
                END
            """,
                        r["unique_id"],  # WHERE
                        r["unique_id"],  # INSERT
                        r["source_name"],
                        r["account_name"],
                        r["code"],
                        sql_safe_number(r["end_of_period"]),
                        sql_safe_number(r["begin_of_period"]),
                        r["report_type"],
                        r["period"],
                        r["created_date"],
                        )

        cur.execute("""
            INSERT INTO dbo.etl_file_log(file_id,file_name,modified_time,md5_checksum)
            VALUES (?,?,?,?)
        """,
            file_id,
            file_name,
            f.get("modifiedTime"),
            f.get("md5Checksum")
        )

        conn.commit()

        try:
            os.remove(tmp_path)
        except Exception:
            pass

    cur.close()
    conn.close()


# ================= TASK 2: LANDING -> STAGING =================
def landing_to_staging():
    # đọc landing
    conn_landing = get_connection("landing_finance")
    df = pd.read_sql("""
        SELECT
            code,
            period,
            end_of_period,
            begin_of_period,
            report_type,
            created_date
        FROM dbo.Can_doi_ke_toan
        WHERE report_type = ?
    """, conn_landing, params=[REPORT_TYPE])
    conn_landing.close()

    if df.empty:
        return

    # (1) lọc null/0
    df = df[~(
        ((df["end_of_period"].isna()) | (df["end_of_period"] == 0)) &
        ((df["begin_of_period"].isna()) | (df["begin_of_period"] == 0))
    )].copy()

    if df.empty:
        return

    df["code"] = df["code"].astype(str).str.strip()
    df["period"] = df["period"].astype(int)
    df["row_id"] = df.apply(lambda x: md5(f"{x['code']}|{x['period']}|{x['report_type']}"), axis=1)


    df = df.sort_values("created_date").drop_duplicates(subset=["row_id"], keep="last")

    conn_stg = get_connection("Staging_Finance")
    cur = conn_stg.cursor()

    df_old = pd.read_sql("SELECT row_id FROM dbo.stg_fact_can_doi_ke_toan_s", conn_stg)
    old_ids = set(df_old["row_id"].astype(str)) if not df_old.empty else set()

    to_insert = df[~df["row_id"].isin(old_ids)].copy()
    if to_insert.empty:
        cur.close()
        conn_stg.close()
        return

    for _, r in to_insert.iterrows():
        cur.execute("""
            INSERT INTO dbo.stg_fact_can_doi_ke_toan_s
            (
                row_id,
                account_nk,
                period_date,
                end_value,
                begin_value,
                report_type,
                created_date
            )
            VALUES (?,?,?,?,?,?,?)
        """,
            r["row_id"],
            r["code"],
            int(r["period"]),
            sql_safe_number(r["end_of_period"]),
            sql_safe_number(r["begin_of_period"]),
            r["report_type"],
            r["created_date"]
        )

    conn_stg.commit()
    cur.close()
    conn_stg.close()


# ================= TASK 3: STAGING -> ATOMIC
def staging_to_atomic_soft_delete():
    # read staging
    conn_stg = get_connection("Staging_Finance")
    df_new = pd.read_sql("""
        SELECT
            row_id,
            account_nk,
            period_date,
            end_value,
            begin_value,
            report_type,
            created_date
        FROM dbo.stg_fact_can_doi_ke_toan_s
        WHERE report_type = ?
    """, conn_stg, params=[REPORT_TYPE])
    conn_stg.close()

    if df_new.empty:
        return

    df_new["row_id"] = df_new["row_id"].astype(str)
    new_ids = set(df_new["row_id"])

    conn_atm = get_connection("Atomic_Finance")
    df_old = pd.read_sql("""
        SELECT
            row_id,
            end_value,
            begin_value,
            is_delete
        FROM dbo.fact_balance_sheet
    """, conn_atm)

    df_old["row_id"] = df_old["row_id"].astype(str) if not df_old.empty else df_old
    old_ids = set(df_old["row_id"]) if not df_old.empty else set()

    cur = conn_atm.cursor()
    now = datetime.now()


    to_insert = df_new[~df_new["row_id"].isin(old_ids)]
    for _, r in to_insert.iterrows():
        cur.execute("""
            INSERT INTO dbo.fact_balance_sheet
            (
                row_id,
                account_nk,
                date_sk,
                end_value,
                begin_value,
                created_date,
                updated_at,
                is_delete
            )
            VALUES (?,?,?,?,?,?,?,0)
        """,
            r["row_id"],
            int(r["account_nk"]),
            int(r["period_date"]),
            sql_safe_number(r["end_value"]),
            sql_safe_number(r["begin_value"]),
            r["created_date"],
            now
        )

    # UPDATE
    if not df_old.empty:
        merged = df_new.merge(df_old, on="row_id", suffixes=("_n", "_o"), how="inner")

        def norm(v):
            if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                return None
            return v

        changed_mask = (
            merged["end_value_n"].apply(norm) != merged["end_value_o"].apply(norm)
        ) | (
            merged["begin_value_n"].apply(norm) != merged["begin_value_o"].apply(norm)
        ) | (
            merged["is_delete"] != 0
        )

        changed = merged[changed_mask]

        for _, r in changed.iterrows():
            cur.execute("""
                UPDATE dbo.fact_balance_sheet
                SET end_value = ?,
                    begin_value = ?,
                    updated_at = ?,
                    is_delete = 0
                WHERE row_id = ?
            """,
                sql_safe_number(r["end_value_n"]),
                sql_safe_number(r["begin_value_n"]),
                now,
                r["row_id"]
            )

    #  DELETE
    to_soft_delete = old_ids - new_ids
    for rid in to_soft_delete:
        cur.execute("""
            UPDATE dbo.fact_balance_sheet
            SET is_delete = 1,
                updated_at = ?
            WHERE row_id = ?
              AND is_delete = 0
        """, now, rid)

    conn_atm.commit()
    cur.close()
    conn_atm.close()


# ================= DAG =================
default_args = {
    "owner": "bac",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_cdkt",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["cdkt", "drive", "landing", "staging", "atomic"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_helper_tables",
        python_callable=ensure_helper_tables,
    )

    t_filter = PythonOperator(
        task_id="filter_new_drive_files",
        python_callable=filter_new_drive_files,
        provide_context=True,
    )

    t_drive = PythonOperator(
        task_id="drive_to_landing",
        python_callable=drive_to_landing,
        provide_context=True,
    )

    t_stg = PythonOperator(
        task_id="landing_to_staging",
        python_callable=landing_to_staging,
    )

    t_atomic = PythonOperator(
        task_id="staging_to_atomic_soft_delete",
        python_callable=staging_to_atomic_soft_delete,
    )

    t_ensure >> t_filter >> t_drive >> t_stg >> t_atomic
