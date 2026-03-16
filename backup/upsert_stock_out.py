import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\stock_out.xlsx"

SCHEMA = [
    "order_date",
    "order_id",
    "order_status",
    "sale_staff_name",
    "customer_code",
    "customer_name",
    "good_code",
    "ordered_quantity",
    "delivered_quantity",
    "remaining_quantity",
    "expected_revenue",
    "other_conditions",
    "deposit_amount",
    "realized_revenue",
    "unrealized_revenue",
    "note",
    "profitability",
    "payment"
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
    df = df[df["order_date"] != "Tổng cộng"]

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

    print("Truncating table stock_out...")

    cur.execute("TRUNCATE TABLE stock_out")

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO stock_out (
        order_date,
        order_id,
        order_status,
        sale_staff_name,
        customer_code,
        customer_name,
        good_code,
        ordered_quantity,
        delivered_quantity,
        remaining_quantity,
        expected_revenue,
        other_conditions,
        deposit_amount,
        realized_revenue,
        unrealized_revenue,
        note,
        profitability,
        payment
    )
    VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s
    )
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
# MAIN
# -----------------------------

def run():

    print("Reading Excel row 6 onwards...")

    df = read_excel()

    print("Rows loaded:", len(df))

    print("Loading to database...")

    load_data(df)

    print("Pipeline completed")


if __name__ == "__main__":
    run()