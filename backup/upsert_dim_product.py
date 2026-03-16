import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -------------------------------
# CONFIG
# -------------------------------

EXCEL_FILE = r"C:\Projects\ADG\DWH\raw-data\Danh_sach_hang_hoa_dich_vu.xlsx"

SCHEMA = [
    "index",
    "product_code",
    "product_name",
    "has_tax_deduction",
    "product_group",
    "chemical_material_group",
    "measurement_unit",
    "inventory_quantity",
    "inventory_value",
    "warranty_period",
    "inventory_quantity_threshold",
    "source",
    "description",
    "purchase_note",
    "selling_note",
    "signature_output",
    "default_storage_code",
    "default_storage_name",
    "account_storage_code",
    "account_revenue_code",
    "account_discount_code",
    "account_markdown_code",
    "account_cashback_code",
    "account_cost_code",
    "purchase_discount_percentage",
    "default_purchase_unit_price",
    "latest_purchase_unit_price",
    "selling_unit_price_1",
    "selling_unit_price_2",
    "selling_unit_price_3",
    "default_selling_unit_price",
    "is_after_tax",
    "VAT_percentage",
    "other_tax_percentage",
    "import_tax_percentage",
    "export_tax_percentage",
    "good_services_subject_to_excise_tax_group",
    "extended_field_1",
    "extended_field_2",
    "extended_field_3",
    "extended_field_4",
    "extended_field_5",
    "status",
    "from_quantity",
    "to_quantity",
    "selling_discount_percentage",
    "discount_value",
    "conversion_unit",
    "conversion_percentage",
    "calculation_method",
    "conversion_description",
    "conversion_unit_price_1",
    "conversion_unit_price_2",
    "conversion_unit_price_3",
    "default_conversion_unit_price",
    "material_code",
    "material_name",
    "material_measurement_unit",
    "material_quantity",
    "cost_category",
    "specification_name",
    "allow_duplicate"
]

# -----------------------------
# READ EXCEL FROM ROW 6
# -----------------------------

def read_excel():

    df = pd.read_excel(
        EXCEL_FILE,
        skiprows=4,
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
        FROM dim_product
    """)

    existing_keys = set(r[0] for r in cur.fetchall())

    insert_rows = []
    update_rows = []

    # -----------------------------
    # SPLIT INSERT / UPDATE
    # -----------------------------

    for row in df.itertuples(index=False):

        key = row.index

        values = tuple(row)

        if key in existing_keys:
            update_rows.append(values)
        else:
            insert_rows.append(values)

    # -----------------------------
    # INSERT
    # -----------------------------

    insert_sql = """
    INSERT INTO dim_product (
        {}
    )
    VALUES ({})
    """.format(
        ",".join(['"index"'] + SCHEMA[1:]),
        ",".join(["%s"] * len(SCHEMA))
    )

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
    UPDATE dim_product
    SET
        product_code = %s,
        product_name = %s,
        has_tax_deduction = %s,
        product_group = %s,
        chemical_material_group = %s,
        measurement_unit = %s,
        inventory_quantity = %s,
        inventory_value = %s,
        warranty_period = %s,
        inventory_quantity_threshold = %s,
        source = %s,
        description = %s,
        purchase_note = %s,
        selling_note = %s,
        signature_output = %s,
        default_storage_code = %s,
        default_storage_name = %s,
        account_storage_code = %s,
        account_revenue_code = %s,
        account_discount_code = %s,
        account_markdown_code = %s,
        account_cashback_code = %s,
        account_cost_code = %s,
        purchase_discount_percentage = %s,
        default_purchase_unit_price = %s,
        latest_purchase_unit_price = %s,
        selling_unit_price_1 = %s,
        selling_unit_price_2 = %s,
        selling_unit_price_3 = %s,
        default_selling_unit_price = %s,
        is_after_tax = %s,
        VAT_percentage = %s,
        other_tax_percentage = %s,
        import_tax_percentage = %s,
        export_tax_percentage = %s,
        good_services_subject_to_excise_tax_group = %s,
        extended_field_1 = %s,
        extended_field_2 = %s,
        extended_field_3 = %s,
        extended_field_4 = %s,
        extended_field_5 = %s,
        status = %s,
        from_quantity = %s,
        to_quantity = %s,
        selling_discount_percentage = %s,
        discount_value = %s,
        conversion_unit = %s,
        conversion_percentage = %s,
        calculation_method = %s,
        conversion_description = %s,
        conversion_unit_price_1 = %s,
        conversion_unit_price_2 = %s,
        conversion_unit_price_3 = %s,
        default_conversion_unit_price = %s,
        material_code = %s,
        material_name = %s,
        material_measurement_unit = %s,
        material_quantity = %s,
        cost_category = %s,
        specification_name = %s,
        allow_duplicate = %s,
        updated_at = NOW()
    WHERE
        "index" = %s
    """

    update_values = []

    for row in update_rows:

        update_values.append(row[1:] + (row[0],))

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

    print("Reading Excel row 5 onwards...")

    df = read_excel()

    print("Rows loaded from Excel:", len(df))

    print("Running UPSERT pipeline...")

    upsert_data(df)

    print("Pipeline completed")


if __name__ == "__main__":
    run()