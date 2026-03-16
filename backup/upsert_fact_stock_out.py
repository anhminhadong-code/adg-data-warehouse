import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\stock_remaining.xlsx"

SCHEMA = [
    "storage_name",
    "good_code",
    "stock_in_quantity",
    "stock_out_quantity",
    "stock_remaining_quantity"
]

# -----------------------------
# READ EXCEL FROM ROW 6
# -----------------------------

def read_excel():

    df = pd.read_excel(
        EXCEL_FILE,
        skiprows=5,
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
        SELECT storage_name, good_code
        FROM stock_remaining
    """)

    existing_keys = set(cur.fetchall())

    insert_rows = []
    update_rows = []

    # -----------------------------
    # SPLIT INSERT / UPDATE
    # -----------------------------

    for row in df.itertuples(index=False):

        key = (row.storage_name, row.good_code)

        values = (
            row.storage_name,
            row.good_code,
            row.stock_in_quantity,
            row.stock_out_quantity,
            row.stock_remaining_quantity
        )

        if key in existing_keys:
            update_rows.append(values)
        else:
            insert_rows.append(values)

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO stock_remaining (
        storage_name,
        good_code,
        stock_in_quantity,
        stock_out_quantity,
        stock_remaining_quantity
    )
    VALUES (%s,%s,%s,%s,%s)
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
    UPDATE stock_remaining
    SET
        stock_in_quantity = %s,
        stock_out_quantity = %s,
        stock_remaining_quantity = %s,
        updated_at = NOW()
    WHERE
        storage_name = %s
        AND good_code = %s
    """

    update_values = []

    for row in update_rows:

        update_values.append((
            row[2],  # stock_in_quantity
            row[3],  # stock_out_quantity
            row[4],  # stock_remaining_quantity
            row[0],  # storage_name
            row[1],  # good_code
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

    print("Reading Excel row 6 onwards...")

    df = read_excel()

    print("Rows loaded from Excel:", len(df))

    print("Running UPSERT pipeline...")

    upsert_data(df)

    print("Pipeline completed")


if __name__ == "__main__":
    run()