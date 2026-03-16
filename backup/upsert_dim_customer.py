import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\Danh_sach_khach_hang.xlsx"

SCHEMA = [
    "index",
    "customer_code",
    "customer_name",
    "customer_address",
    "accounts_receivable",
    "tax_identification_number",
    "phone_number",
    "cellphone_number",
    "is_internal_entity"
]

# -----------------------------
# READ EXCEL FROM row 4
# -----------------------------

def read_excel():

    df = pd.read_excel(
        EXCEL_FILE,
        skiprows=3,
        header=None,
        skipfooter=1
    )

    df = df.iloc[:, :len(SCHEMA)]
    df.columns = SCHEMA

    df = df.dropna(how="all")

    return df


# -----------------------------
# UPSERT WITH LOGGING
# -----------------------------

def upsert_data(df):

    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cur = conn.cursor()

    # -----------------------------
    # LOAD EXISTING KEYS
    # -----------------------------

    cur.execute("""
        SELECT customer_code
        FROM dim_customer
    """)

    existing_keys = set(r[0] for r in cur.fetchall())

    insert_rows = []
    update_rows = []

    # -----------------------------
    # SPLIT INSERT / UPDATE
    # -----------------------------

    for row in df.itertuples(index=False):

        key = row.customer_code

        values = (
            row.index,
            row.customer_code,
            row.customer_name,
            row.customer_address,
            row.accounts_receivable,
            row.tax_identification_number,
            row.phone_number,
            row.cellphone_number,
            row.is_internal_entity
        )

        if key in existing_keys:
            update_rows.append(values)
        else:
            insert_rows.append(values)

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO dim_customer (
        "index",
        customer_code,
        customer_name,
        customer_address,
        accounts_receivable,
        tax_identification_number,
        phone_number,
        cellphone_number,
        is_internal_entity
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    if insert_rows:
        psycopg2.extras.execute_batch(
            cur,
            insert_sql,
            insert_rows,
            page_size=1000
        )

    # -----------------------------
    # UPDATE
    # -----------------------------

    update_sql = """
    UPDATE dim_customer
    SET
        "index" = %s,
        customer_name = %s,
        customer_address = %s,
        accounts_receivable = %s,
        tax_identification_number = %s,
        phone_number = %s,
        cellphone_number = %s,
        is_internal_entity = %s,
        updated_at = NOW()
    WHERE
        customer_code = %s
    """

    update_values = []

    for row in update_rows:

        update_values.append((
            row[0],  # index
            row[2],  # customer_name
            row[3],  # customer_address
            row[4],  # accounts_receivable
            row[5],  # tax_identification_number
            row[6],  # phone_number
            row[7],  # cellphone_number
            row[8],  # is_internal_entity
            row[1],  # customer_code
        ))

    if update_values:
        psycopg2.extras.execute_batch(
            cur,
            update_sql,
            update_values,
            page_size=1000
        )

    conn.commit()

    # -----------------------------
    # LOGGING
    # -----------------------------

    print("--------------------------------------------------")
    print("Pipeline execution time :", datetime.now())
    print("Inserted rows           :", len(insert_rows))
    print("Updated rows            :", len(update_rows))
    print("Total processed rows    :", len(insert_rows) + len(update_rows))
    print("--------------------------------------------------")

    cur.close()
    conn.close()


# -----------------------------
# MAIN PIPELINE
# -----------------------------

def run():

    print("Reading Excel row 4 onwards...")

    df = read_excel()

    print("Rows loaded from Excel:", len(df))

    print("Running UPSERT pipeline...")

    upsert_data(df)

    print("Pipeline completed")


if __name__ == "__main__":
    run()