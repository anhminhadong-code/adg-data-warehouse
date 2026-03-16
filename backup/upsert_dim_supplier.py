import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\Danh_sach_nha_cung_cap.xlsx"

SCHEMA = [
    "index",
    "supplier_code",
    "supplier_name",
    "supplier_address",
    "accounts_payable",
    "tax_identification_number",
    "invoice_risk",
    "reference_document",
    "phone_number",
    "is_internal_entity",
    "organization_type"
]

# -----------------------------
# READ EXCEL FROM ROW 4
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
        SELECT "index"
        FROM dim_supplier
    """)

    existing_keys = set(r[0] for r in cur.fetchall())

    insert_rows = []
    update_rows = []

    # -----------------------------
    # SPLIT INSERT / UPDATE
    # -----------------------------

    for row in df.itertuples(index=False):

        key = row.index

        values = (
            row.index,
            row.supplier_code,
            row.supplier_name,
            row.supplier_address,
            row.accounts_payable,
            row.tax_identification_number,
            row.invoice_risk,
            row.reference_document,
            row.phone_number,
            row.is_internal_entity,
            row.organization_type
        )

        if key in existing_keys:
            update_rows.append(values)
        else:
            insert_rows.append(values)

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO dim_supplier (
        "index",
        supplier_code,
        supplier_name,
        supplier_address,
        accounts_payable,
        tax_identification_number,
        invoice_risk,
        reference_document,
        phone_number,
        is_internal_entity,
        organization_type
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
    UPDATE dim_supplier
    SET
        supplier_code = %s,
        supplier_name = %s,
        supplier_address = %s,
        accounts_payable = %s,
        tax_identification_number = %s,
        invoice_risk = %s,
        reference_document = %s,
        phone_number = %s,
        is_internal_entity = %s,
        organization_type = %s,
        updated_at = NOW()
    WHERE
        "index" = %s
    """

    update_values = []

    for row in update_rows:

        update_values.append((
            row[1],  # supplier_code
            row[2],  # supplier_name
            row[3],  # supplier_address
            row[4],  # accounts_payable
            row[5],  # tax_identification_number
            row[6],  # invoice_risk
            row[7],  # reference_document
            row[8],  # phone_number
            row[9],  # is_internal_entity
            row[10], # organization_type
            row[0]   # index
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