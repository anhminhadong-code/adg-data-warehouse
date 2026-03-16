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
# LOAD DATA (TRUNCATE + INSERT)
# -----------------------------

def load_data(df):

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
    # TRUNCATE TABLE
    # -----------------------------

    print("Truncating table stock_remaining...")

    cur.execute("TRUNCATE TABLE stock_remaining")

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

    data = df.values.tolist()

    psycopg2.extras.execute_batch(
        cur,
        insert_sql,
        data,
        page_size=1000
    )

    conn.commit()

    print("--------------------------------------------------")
    print("Pipeline execution time :", datetime.now())
    print("Inserted rows           :", len(data))
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

    print("Loading data to database...")

    load_data(df)

    print("Pipeline completed")


if __name__ == "__main__":
    run()