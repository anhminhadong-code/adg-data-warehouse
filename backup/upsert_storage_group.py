import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\storage_group.xlsx"

SCHEMA = [
    "storage_name",
    "storage_group_name"
]

# -----------------------------
# READ EXCEL FROM ROW 6
# -----------------------------

def read_excel():

    df = pd.read_excel(
        EXCEL_FILE,
        skiprows=1,
        header=None
    )
    
    df = df.iloc[:, :len(SCHEMA)]
    df.columns = SCHEMA

    df = df.dropna(how="all")
    for col in df.select_dtypes(include="object").columns:
        max_len = df[col].astype(str).str.len().max()
        print(col, max_len)

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
        SELECT storage_name
        FROM dim_storage_group
    """)

    existing_keys = set(cur.fetchall())

    insert_rows = []
    update_rows = []

    # -----------------------------
    # SPLIT INSERT / UPDATE
    # -----------------------------

    for row in df.itertuples(index=False):

        key = (row.storage_name,)

        values = (
            row.storage_name,
            row.storage_group_name
        )

        if key in existing_keys:
            update_rows.append(values)
        else:
            insert_rows.append(values)

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO dim_storage_group (
        storage_name,
        storage_group_name
    )
    VALUES (%s,%s)
    """

    if insert_rows:
        psycopg2.extras.execute_batch(
            cur,
            insert_sql,
            insert_rows,
            page_size=100
        )

    # -----------------------------
    # UPDATE
    # -----------------------------

    update_sql = """
    UPDATE dim_storage_group
    SET
        storage_group_name = %s
    WHERE
        storage_name = %s
    """

    update_values = []

    for row in update_rows:

        update_values.append((
            row[1],  # storage_group_name
            row[0],  # storage_name
        ))

    if update_values:
        psycopg2.extras.execute_batch(
            cur,
            update_sql,
            update_values,
            page_size=100
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