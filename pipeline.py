import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

# -----------------------------------------------
# CONFIG
# -----------------------------------------------

BASE = r"C:\Projects\ADG\DWH\raw-data"

# mode options:
#   "upsert"   — load existing keys, batch INSERT new + batch UPDATE existing
#   "conflict" — INSERT ... ON CONFLICT (key_cols) DO UPDATE SET ...
#   "truncate" — TRUNCATE table then INSERT all rows

PIPELINES = [

    # --- DIMENSIONS ---

    {
        "label":         "dim_measurement_unit",
        "excel_file":    rf"{BASE}\Danh_sach_don_vi_tinh.xlsx",
        "skiprows":      3,
        "skipfooter":    1,
        "schema":        ["index", "measurement_unit", "description", "status"],
        "table":         "dim_measurement_unit",
        "key_cols":      ["measurement_unit"],
        "mode":          "conflict",
        "updated_at":    True,
    },
    {
        "label":         "dim_storage",
        "excel_file":    rf"{BASE}\Danh_sach_kho.xlsx",
        "skiprows":      3,
        "schema":        ["index", "storage_code", "storage_name", "storage_address", "status"],
        "table":         "dim_storage",
        "key_cols":      ["index"],
        "mode":          "upsert",
        "updated_at":    False,
        "print_max_len": True,
    },
    {
        "label":         "dim_storage_group",
        "excel_file":    rf"{BASE}\storage_group.xlsx",
        "skiprows":      1,
        "schema":        ["storage_name", "storage_group_name"],
        "table":         "dim_storage_group",
        "key_cols":      ["storage_name"],
        "mode":          "upsert",
        "updated_at":    False,
        "print_max_len": True,
    },
    {
        "label":         "dim_storage_group_category",
        "excel_file":    rf"{BASE}\storage_group_category.xlsx",
        "skiprows":      1,
        "schema":        ["storage_group_name", "storage_group_category"],
        "table":         "dim_storage_group_category",
        "key_cols":      ["storage_group_name"],
        "mode":          "upsert",
        "updated_at":    False,
        "print_max_len": True,
    },
    {
        "label":         "dim_supplier",
        "excel_file":    rf"{BASE}\Danh_sach_nha_cung_cap.xlsx",
        "skiprows":      3,
        "skipfooter":    1,
        "schema":        [
            "index", "supplier_code", "supplier_name", "supplier_address",
            "accounts_payable", "tax_identification_number", "invoice_risk",
            "reference_document", "phone_number", "is_internal_entity", "organization_type"
        ],
        "table":         "dim_supplier",
        "key_cols":      ["index"],
        "mode":          "upsert",
        "updated_at":    True,
        "print_max_len": True,
    },
    {
        "label":         "dim_customer",
        "excel_file":    rf"{BASE}\Danh_sach_khach_hang.xlsx",
        "skiprows":      3,
        "skipfooter":    1,
        "schema":        [
            "index", "customer_code", "customer_name", "customer_address",
            "accounts_receivable", "tax_identification_number",
            "phone_number", "cellphone_number", "is_internal_entity"
        ],
        "table":         "dim_customer",
        "key_cols":      ["customer_code"],
        "mode":          "upsert",
        "updated_at":    True,
    },
    {
        "label":         "dim_product",
        "excel_file":    rf"{BASE}\Danh_sach_hang_hoa_dich_vu.xlsx",
        "skiprows":      4,
        "skipfooter":    1,
        "schema":        [
            "index", "product_code", "product_name", "has_tax_deduction",
            "product_group", "chemical_material_group", "measurement_unit",
            "inventory_quantity", "inventory_value", "warranty_period",
            "inventory_quantity_threshold", "source", "description",
            "purchase_note", "selling_note", "signature_output",
            "default_storage_code", "default_storage_name",
            "account_storage_code", "account_revenue_code",
            "account_discount_code", "account_markdown_code",
            "account_cashback_code", "account_cost_code",
            "purchase_discount_percentage", "default_purchase_unit_price",
            "latest_purchase_unit_price", "selling_unit_price_1",
            "selling_unit_price_2", "selling_unit_price_3",
            "default_selling_unit_price", "is_after_tax", "VAT_percentage",
            "other_tax_percentage", "import_tax_percentage",
            "export_tax_percentage",
            "good_services_subject_to_excise_tax_group",
            "extended_field_1", "extended_field_2", "extended_field_3",
            "extended_field_4", "extended_field_5", "status",
            "from_quantity", "to_quantity", "selling_discount_percentage",
            "discount_value", "conversion_unit", "conversion_percentage",
            "calculation_method", "conversion_description",
            "conversion_unit_price_1", "conversion_unit_price_2",
            "conversion_unit_price_3", "default_conversion_unit_price",
            "material_code", "material_name", "material_measurement_unit",
            "material_quantity", "cost_category", "specification_name",
            "allow_duplicate"
        ],
        "table":         "dim_product",
        "key_cols":      ["index"],
        "mode":          "upsert",
        "updated_at":    True,
        "print_max_len": True,
    },

    # --- OPERATIONAL ---

    {
        "label":      "stock_in",
        "excel_file": rf"{BASE}\stock_in.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "bank_code", "order_id", "supplier_code", "good_code",
            "ordered_quantity", "delivered_quantity", "remaining_quantity",
            "unit_price", "purchase_value", "order_date", "order_status",
            "expected_deliver_date", "note", "registered_storage",
            "payment_due_date", "payment_check", "sale_contract",
            "storage_due_date"
        ],
        "table":      "stock_in",
        "key_cols":   ["order_id", "supplier_code", "good_code"],
        "mode":       "upsert",
        "updated_at": True,
        "preprocess": "stock_in",
    },
    {
        "label":      "stock_out",
        "excel_file": rf"{BASE}\stock_out.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "order_date", "order_id", "order_status", "sale_staff_name",
            "customer_code", "customer_name", "good_code",
            "ordered_quantity", "delivered_quantity", "remaining_quantity",
            "expected_revenue", "other_conditions", "deposit_amount",
            "realized_revenue", "unrealized_revenue", "note",
            "profitability", "payment"
        ],
        "table":      "stock_out",
        "mode":       "truncate",
        "preprocess": "stock_out",
    },
    {
        "label":      "stock_remaining",
        "excel_file": rf"{BASE}\stock_remaining.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "storage_name", "good_code", "stock_in_quantity",
            "stock_out_quantity", "stock_remaining_quantity"
        ],
        "table":      "stock_remaining",
        "mode":       "truncate",
    },

    # --- FACTS (placeholders — update excel_file/schema/table when ready) ---

    {
        "label":      "fact_order",
        "excel_file": rf"{BASE}\stock_remaining.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "storage_name", "good_code", "stock_in_quantity",
            "stock_out_quantity", "stock_remaining_quantity"
        ],
        "table":      "stock_remaining",
        "key_cols":   ["storage_name", "good_code"],
        "mode":       "upsert",
        "updated_at": True,
    },
    {
        "label":      "fact_stock_in",
        "excel_file": rf"{BASE}\stock_remaining.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "storage_name", "good_code", "stock_in_quantity",
            "stock_out_quantity", "stock_remaining_quantity"
        ],
        "table":      "stock_remaining",
        "key_cols":   ["storage_name", "good_code"],
        "mode":       "upsert",
        "updated_at": True,
    },
    {
        "label":      "fact_stock_move",
        "excel_file": rf"{BASE}\stock_remaining.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "storage_name", "good_code", "stock_in_quantity",
            "stock_out_quantity", "stock_remaining_quantity"
        ],
        "table":      "stock_remaining",
        "key_cols":   ["storage_name", "good_code"],
        "mode":       "upsert",
        "updated_at": True,
    },
    {
        "label":      "fact_stock_out",
        "excel_file": rf"{BASE}\stock_remaining.xlsx",
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "storage_name", "good_code", "stock_in_quantity",
            "stock_out_quantity", "stock_remaining_quantity"
        ],
        "table":      "stock_remaining",
        "key_cols":   ["storage_name", "good_code"],
        "mode":       "upsert",
        "updated_at": True,
    },

]

# -----------------------------------------------
# HELPERS
# -----------------------------------------------

def q(col):
    """Quote reserved SQL keywords."""
    return f'"{col}"' if col == "index" else col


def get_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# -----------------------------------------------
# READ EXCEL
# -----------------------------------------------

def read_excel(cfg):

    df = pd.read_excel(
        cfg["excel_file"],
        skiprows=cfg.get("skiprows", 0),
        header=None,
        skipfooter=cfg.get("skipfooter", 0)
    )

    schema = cfg["schema"]
    df = df.iloc[:, :len(schema)]
    df.columns = schema
    df = df.dropna(how="all")

    # named preprocessing
    preprocess = cfg.get("preprocess")

    if preprocess == "stock_in":
        for col in ["order_date", "expected_deliver_date", "payment_due_date", "storage_due_date"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        df = df.replace({pd.NaT: None})
        df = df.where(pd.notnull(df), None)
        df = df[df["bank_code"] != "Tổng cộng"]

    elif preprocess == "stock_out":
        df = df[df["order_date"] != "Tổng cộng"]

    if cfg.get("print_max_len"):
        for col in df.select_dtypes(include="object").columns:
            print(f"  {col}: {df[col].astype(str).str.len().max()}")

    return df

# -----------------------------------------------
# UPSERT / LOAD
# -----------------------------------------------

def run_pipeline(cfg):

    label  = cfg["label"]
    table  = cfg["table"]
    schema = cfg["schema"]
    mode   = cfg.get("mode", "upsert")

    print(f"\n{'='*50}")
    print(f"Pipeline : {label}")
    print(f"Table    : {table}")

    df = read_excel(cfg)
    print(f"Rows loaded from Excel: {len(df)}")

    cols_sql     = ", ".join(q(c) for c in schema)
    placeholders = ", ".join(["%s"] * len(schema))

    conn = get_connection()
    cur  = conn.cursor()

    # ---------------------------
    # TRUNCATE + INSERT
    # ---------------------------

    if mode == "truncate":

        print(f"Truncating {table}...")
        cur.execute(f"TRUNCATE TABLE {table}")

        insert_sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        data = df.values.tolist()

        psycopg2.extras.execute_batch(cur, insert_sql, data, page_size=1000)
        conn.commit()

        print("--------------------------------------------------")
        print("Pipeline execution time :", datetime.now())
        print("Inserted rows           :", len(data))
        print("--------------------------------------------------")

    # ---------------------------
    # ON CONFLICT DO UPDATE
    # ---------------------------

    elif mode == "conflict":

        key_cols  = cfg["key_cols"]
        non_key   = [c for c in schema if c not in key_cols]
        set_parts = [f"{q(c)} = EXCLUDED.{q(c)}" for c in non_key]

        if cfg.get("updated_at"):
            set_parts.append("updated_at = NOW()")

        conflict_cols = ", ".join(q(c) for c in key_cols)
        set_clause    = ", ".join(set_parts)

        insert_sql = f"""
        INSERT INTO {table} ({cols_sql})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {set_clause}
        """

        rows = [tuple(r) for r in df.itertuples(index=False)]
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=1000)
        conn.commit()

        print("--------------------------------------------------")
        print("Pipeline execution time :", datetime.now())
        print("Processed rows          :", len(rows))
        print("--------------------------------------------------")

    # ---------------------------
    # UPSERT (key-based split)
    # ---------------------------

    elif mode == "upsert":

        key_cols      = cfg["key_cols"]
        key_cols_sql  = ", ".join(q(c) for c in key_cols)

        cur.execute(f"SELECT {key_cols_sql} FROM {table}")
        existing_keys = set(cur.fetchall())

        insert_rows = []
        update_rows = []

        for row in df.itertuples(index=False):

            key    = tuple(getattr(row, c) for c in key_cols)
            values = tuple(row)

            if key in existing_keys:
                update_rows.append(values)
            else:
                insert_rows.append(values)

        # INSERT
        insert_sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

        if insert_rows:
            psycopg2.extras.execute_batch(cur, insert_sql, insert_rows, page_size=1000)

        # UPDATE
        non_key   = [c for c in schema if c not in key_cols]
        set_parts = [f"{q(c)} = %s" for c in non_key]

        if cfg.get("updated_at"):
            set_parts.append("updated_at = NOW()")

        where_parts = [f"{q(c)} = %s" for c in key_cols]
        update_sql  = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"

        if update_rows:
            row_dicts     = [dict(zip(schema, r)) for r in update_rows]
            update_values = [
                tuple(d[c] for c in non_key) + tuple(d[c] for c in key_cols)
                for d in row_dicts
            ]
            psycopg2.extras.execute_batch(cur, update_sql, update_values, page_size=1000)

        conn.commit()

        print("--------------------------------------------------")
        print("Pipeline execution time :", datetime.now())
        print("Inserted rows           :", len(insert_rows))
        print("Updated rows            :", len(update_rows))
        print("Total processed rows    :", len(insert_rows) + len(update_rows))
        print("--------------------------------------------------")

    cur.close()
    conn.close()

# -----------------------------------------------
# MAIN
# -----------------------------------------------

def run():
    for cfg in PIPELINES:
        run_pipeline(cfg)
    print("\nAll pipelines completed.")


if __name__ == "__main__":
    run()
