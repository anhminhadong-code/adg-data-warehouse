import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\stock_in.xlsx"

SCHEMA = [
    "bank_code",
    "order_id",
    "supplier_code",
    "good_code",
    "ordered_quantity",
    "delivered_quantity",
    "remaining_quantity",
    "unit_price",
    "purchase_value",
    "order_date",
    "order_status",
    "expected_deliver_date",
    "note",
    "registered_storage",
    "payment_due_date",
    "payment_check",
    "sale_contract",
    "storage_due_date"
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

    # convert datetime columns
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["expected_deliver_date"] = pd.to_datetime(df["expected_deliver_date"], errors="coerce")
    df["payment_due_date"] = pd.to_datetime(df["payment_due_date"], errors="coerce")
    df["storage_due_date"] = pd.to_datetime(df["storage_due_date"], errors="coerce")

    # convert NaN/NaT -> None
    df = df.replace({pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    df = df[df["bank_code"] != "Tổng cộng"]
    # print(df[["order_date", "expected_deliver_date"]].head())

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
        SELECT order_id, supplier_code, good_code
        FROM stock_in
    """)

    existing_keys = set(cur.fetchall())

    insert_rows = []
    update_rows = []

    # -----------------------------
    # SPLIT INSERT / UPDATE
    # -----------------------------

    for row in df.itertuples(index=False):

        key = (row.order_id, row.supplier_code, row.good_code)

        values = (
            row.bank_code,
            row.order_id,
            row.supplier_code,
            row.good_code,
            row.ordered_quantity,
            row.delivered_quantity,
            row.remaining_quantity,
            row.unit_price,
            row.purchase_value,
            row.order_date,
            row.order_status,
            row.expected_deliver_date,
            row.note,
            row.registered_storage,
            row.payment_due_date,
            row.payment_check,
            row.sale_contract,
            row.storage_due_date
        )

        if key in existing_keys:
            update_rows.append(values)
        else:
            insert_rows.append(values)

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO stock_in (
        bank_code,
        order_id,
        supplier_code,
        good_code,
        ordered_quantity,
        delivered_quantity,
        remaining_quantity,
        unit_price,
        purchase_value,
        order_date,
        order_status,
        expected_deliver_date,
        note,
        registered_storage, 
        payment_due_date,
        payment_check,
        sale_contract,
        storage_due_date
    )
    VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s
    )
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
    UPDATE stock_in
    SET
        bank_code=%s,
        ordered_quantity=%s,
        delivered_quantity=%s,
        remaining_quantity=%s,
        unit_price=%s,
        purchase_value=%s,
        order_date=%s,
        order_status=%s,
        expected_deliver_date=%s,
        note=%s,
        registered_storage=%s,
        payment_due_date=%s,
        payment_check=%s,
        sale_contract=%s,
        storage_due_date=%s,
        updated_at = NOW()
    WHERE
        order_id=%s
        AND supplier_code=%s
        AND good_code=%s
    """

    update_values = []

    for row in update_rows:

        update_values.append((
            row[0],   # bank_code
            row[4],   # quantity
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
            row[12],
            row[13],
            row[14],
            row[15],
            row[16],
            row[17],
            row[1],   # order_id
            row[2],   # supplier_code
            row[3],   # good_code
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