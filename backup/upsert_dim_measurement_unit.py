import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\Danh_sach_don_vi_tinh.xlsx"

SCHEMA = [
    "index",
    "measurement_unit",
    "description",
    "status"
]

# -----------------------------
# READ EXCEL
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
# UPSERT
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

    rows = [
        (
            r.index,
            r.measurement_unit,
            r.description,
            r.status
        )
        for r in df.itertuples(index=False)
    ]

    sql = """
    INSERT INTO dim_measurement_unit (
        "index",
        measurement_unit,
        description,
        status
    )
    VALUES (%s,%s,%s,%s)

    ON CONFLICT (measurement_unit)
    DO UPDATE SET
        "index" = EXCLUDED."index",
        description = EXCLUDED.description,
        status = EXCLUDED.status,
        updated_at = NOW()
    """

    psycopg2.extras.execute_batch(
        cur,
        sql,
        rows,
        page_size=1000
    )

    conn.commit()

    print("-----------------------------------")
    print("Execution time:", datetime.now())
    print("Rows processed:", len(rows))
    print("-----------------------------------")

    cur.close()
    conn.close()


# -----------------------------
# MAIN
# -----------------------------

def run():

    print("Reading Excel...")

    df = read_excel()

    print("Rows loaded:", len(df))

    upsert_data(df)

    print("Pipeline completed")


if __name__ == "__main__":
    run()