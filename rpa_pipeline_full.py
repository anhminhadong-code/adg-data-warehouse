"""
rpa_pipeline_full.py
====================
End-to-end pipeline:
  Phase 1  – RPA: log in to MISA, download Excel exports
  Phase 2  – Rename: move files into the raw-data folder with expected names
  Phase 3  – Ingest: read every Excel and upsert/truncate into PostgreSQL

Usage:
    python rpa_pipeline_full.py              # run all three phases
    python rpa_pipeline_full.py --ingest-only  # skip RPA, just ingest
    python rpa_pipeline_full.py --rpa-only     # download only, skip ingest
"""

import os
import re
import sys
import json
import time
import shutil
import hashlib
import imaplib
import email
import logging
import argparse
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql as pgsql
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# =============================================================================
# ENV
# =============================================================================

load_dotenv()

MISA_USERNAME = os.getenv("MISA_USERNAME")
MISA_PASSWORD = os.getenv("MISA_PASSWORD")

EMAIL_USER  = os.getenv("OTP_EMAIL")
EMAIL_PASS  = os.getenv("OTP_PASSWORD")
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")

BASE_URL  = "https://actapp.misa.vn"
HEADLESS  = os.getenv("HEADLESS", "false").lower() == "true"

# =============================================================================
# DIRECTORIES
# =============================================================================

DOWNLOAD_DIR  = os.path.abspath("misa_reports")
RAW_DATA_DIR  = os.getenv("RAW_DATA_DIR", os.path.abspath("raw-data"))
LOG_DIR       = os.path.abspath("logs")
CAPTURE_DIR   = os.path.abspath("misa_api_capture")

for d in (DOWNLOAD_DIR, RAW_DATA_DIR, LOG_DIR, CAPTURE_DIR):
    os.makedirs(d, exist_ok=True)

REQUEST_LOG  = os.path.join(CAPTURE_DIR, "requests.jsonl")
RESPONSE_LOG = os.path.join(CAPTURE_DIR, "responses.jsonl")
TOKEN_LOG    = os.path.join(CAPTURE_DIR, "tokens.jsonl")

# =============================================================================
# LOGGING
# =============================================================================

LOG_FILE = os.path.join(LOG_DIR, "misa_rpa.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()

# =============================================================================
# MODULE → EXPORT CONFIG
#
# Each entry describes one MISA module page and the export button(s) to click.
#
# Keys per module:
#   url       – full page URL (confirmed from browser inspection)
#   label     – human-readable name for logs
#   exports   – list of export actions, in click order
#
# Keys per export action:
#   selector      – CSS selector for the export element (preferred)
#   button_text   – fallback: filter <button> by visible text (used when
#                   selector is not set)
#   index         – 0-based index when multiple elements match the selector
#   rename_to     – filename written to RAW_DATA_DIR (None = keep original)
#
# Confirmed URLs and selectors (from browser inspection):
#   Suppliers  → /app/PU/PUVendor        → div.mi-excel__nav
#   Products   → /app/SA/SAInventoryItems → button.ms-button-feature
#   Warehouse  → /app/DI/DIStock          → div.mi-excel__nav
#
# TODO: inspect and fill in confirmed URLs/selectors for the remaining modules.
# =============================================================================

MODULES = [
    # ------------------------------------------------------------------ #
    #  CONFIRMED — URL and selector verified from browser inspection       #
    # ------------------------------------------------------------------ #
    {
        "url":   "https://actapp.misa.vn/app/PU/PUVendor",
        "label": "Suppliers (Danh sách nhà cung cấp)",
        "exports": [
            {
                "selector":  "div.mi-excel__nav",
                "index":     0,
                "rename_to": "Danh_sach_nha_cung_cap.xlsx",
            },
        ],
    },
    {
        "url":   "https://actapp.misa.vn/app/SA/SAInventoryItems",
        "label": "Products / Items (Danh sách hàng hoá dịch vụ)",
        "exports": [
            {
                "selector":  "button.ms-button-feature",
                "index":     0,
                "rename_to": "Danh_sach_hang_hoa_dich_vu.xlsx",
            },
        ],
    },
    {
        "url":   "https://actapp.misa.vn/app/DI/DIStock",
        "label": "Warehouses / Storages (Danh sách kho)",
        "exports": [
            {
                "selector":  "div.mi-excel__nav",
                "index":     0,
                "rename_to": "Danh_sach_kho.xlsx",
            },
        ],
    }
]

# =============================================================================
# PIPELINE CONFIG  (mirrors pipeline.py)
# =============================================================================

PIPELINES = [

    # --- DIMENSIONS ---

    {
        "label":      "dim_measurement_unit",
        "excel_file": os.path.join(RAW_DATA_DIR, "Danh_sach_don_vi_tinh.xlsx"),
        "skiprows":   3,
        "skipfooter": 1,
        "schema":     ["index", "measurement_unit", "description", "status"],
        "table":      "dim_measurement_unit",
        "key_cols":   ["measurement_unit"],
        "mode":       "conflict",
        "updated_at": True,
    },
    {
        "label":         "dim_storage",
        "excel_file":    os.path.join(RAW_DATA_DIR, "Danh_sach_kho.xlsx"),
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
        "excel_file":    os.path.join(RAW_DATA_DIR, "storage_group.xlsx"),
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
        "excel_file":    os.path.join(RAW_DATA_DIR, "storage_group_category.xlsx"),
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
        "excel_file":    os.path.join(RAW_DATA_DIR, "Danh_sach_nha_cung_cap.xlsx"),
        "skiprows":      3,
        "skipfooter":    1,
        "schema":        [
            "index", "supplier_code", "supplier_name", "supplier_address",
            "accounts_payable", "tax_identification_number", "invoice_risk",
            "reference_document", "phone_number", "is_internal_entity", "organization_type",
        ],
        "table":         "dim_supplier",
        "key_cols":      ["index"],
        "mode":          "upsert",
        "updated_at":    True,
        "print_max_len": True,
    },
    {
        "label":         "dim_customer",
        "excel_file":    os.path.join(RAW_DATA_DIR, "Danh_sach_khach_hang.xlsx"),
        "skiprows":      3,
        "skipfooter":    1,
        "schema":        [
            "index", "customer_code", "customer_name", "customer_address",
            "accounts_receivable", "tax_identification_number",
            "phone_number", "cellphone_number", "is_internal_entity",
        ],
        "table":         "dim_customer",
        "key_cols":      ["customer_code"],
        "mode":          "upsert",
        "updated_at":    True,
    },
    {
        "label":         "dim_product",
        "excel_file":    os.path.join(RAW_DATA_DIR, "Danh_sach_hang_hoa_dich_vu.xlsx"),
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
            "allow_duplicate",
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
        "excel_file": os.path.join(RAW_DATA_DIR, "stock_in.xlsx"),
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "bank_code", "order_id", "supplier_code", "good_code",
            "ordered_quantity", "delivered_quantity", "remaining_quantity",
            "unit_price", "purchase_value", "order_date", "order_status",
            "expected_deliver_date", "note", "registered_storage",
            "payment_due_date", "payment_check", "sale_contract",
            "storage_due_date",
        ],
        "table":      "stock_in",
        "key_cols":   ["order_id", "supplier_code", "good_code"],
        "mode":       "upsert",
        "updated_at": True,
        "preprocess": "stock_in",
    },
    {
        "label":      "stock_out",
        "excel_file": os.path.join(RAW_DATA_DIR, "stock_out.xlsx"),
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "order_date", "order_id", "order_status", "sale_staff_name",
            "customer_code", "customer_name", "good_code",
            "ordered_quantity", "delivered_quantity", "remaining_quantity",
            "expected_revenue", "other_conditions", "deposit_amount",
            "realized_revenue", "unrealized_revenue", "note",
            "profitability", "payment",
        ],
        "table":      "stock_out",
        "mode":       "truncate",
        "preprocess": "stock_out",
    },
    {
        "label":      "stock_remaining",
        "excel_file": os.path.join(RAW_DATA_DIR, "stock_remaining.xlsx"),
        "skiprows":   5,
        "skipfooter": 1,
        "schema":     [
            "storage_name", "good_code", "stock_in_quantity",
            "stock_out_quantity", "stock_remaining_quantity",
        ],
        "table":      "stock_remaining",
        "mode":       "truncate",
    },
]

# =============================================================================
# UTILITIES
# =============================================================================

def write_jsonl(path, data):
    with open(path, "a", encoding="utf8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


def save_screenshot(page, name):
    path = os.path.join(LOG_DIR, f"{name}.png")
    page.screenshot(path=path)
    logger.info(f"Screenshot saved: {path}")


# =============================================================================
# PHASE 1a — OTP READER
# =============================================================================

def get_latest_otp(timeout=120):
    logger.info("Waiting for OTP email…")
    start = time.time()

    while time.time() - start < timeout:
        mail = None
        try:
            mail = imaplib.IMAP4_SSL(IMAP_SERVER)
            mail.login(EMAIL_USER, EMAIL_PASS)
            mail.select("inbox")

            _, messages = mail.search(None, "(UNSEEN)")

            for num in messages[0].split():
                _, data = mail.fetch(num, "(RFC822)")
                msg = email.message_from_bytes(data[0][1])

                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body += part.get_payload(decode=True).decode(errors="ignore")
                else:
                    body = msg.get_payload(decode=True).decode(errors="ignore")

                otp = re.search(r"\b\d{6}\b", body)
                if otp:
                    code = otp.group()
                    logger.info(f"OTP received: {code}")
                    return code

        except Exception as e:
            logger.error(f"OTP read error: {e}")
        finally:
            if mail is not None:
                try:
                    mail.logout()
                except Exception:
                    pass

        time.sleep(5)

    raise TimeoutError("OTP not received within timeout")


# =============================================================================
# PHASE 1b — RPA: LOGIN
# =============================================================================

def rpa_login(page):
    logger.info("Navigating to login page")
    page.goto(BASE_URL)
    page.wait_for_load_state("networkidle")

    page.locator('input[type="text"]').first.fill(MISA_USERNAME)
    page.locator('input[type="password"]').fill(MISA_PASSWORD)

    logger.info("Submitting credentials")
    page.locator("button").filter(has_text="Đăng nhập").click()
    time.sleep(5)

    # OTP step (may or may not appear)
    try:
        otp_input = page.locator('input[type="text"]').nth(1)
        if otp_input.is_visible():
            otp = get_latest_otp()
            otp_input.fill(otp)
            page.locator("button").filter(has_text="Xác nhận").click()
            page.wait_for_load_state("networkidle")
            logger.info("OTP verified")
    except Exception as e:
        logger.info(f"OTP step skipped: {e}")

    logger.info("Login successful")


# =============================================================================
# PHASE 1c — RPA: DOWNLOAD EXPORTS
# =============================================================================

ELEMENT_TIMEOUT_MS = 15_000  # ms to wait for export button after SPA renders


def rpa_download_module(page, module_cfg):
    """
    Navigate to a module, click each export element in order, and save the
    downloaded file to DOWNLOAD_DIR.  Returns a dict mapping rename_to →
    downloaded filepath (or None on failure).

    Selector resolution order per export entry:
      1. "selector" key  → CSS selector (div.mi-excel__nav, button.ms-button-feature, …)
      2. "button_text"   → fallback: <button> filtered by visible text

    Uses Playwright's wait_for(state="visible") instead of time.sleep so that
    SPA pages that render asynchronously after networkidle are handled correctly.
    """
    url        = module_cfg["url"]
    label      = module_cfg["label"]
    safe_label = re.sub(r"[^\w]", "_", label)
    logger.info(f"  → {label}")
    logger.info(f"    URL: {url}")

    results = {}

    try:
        page.goto(url)
        page.wait_for_load_state("networkidle")
    except Exception as e:
        logger.error(f"    Navigation failed: {e}")
        save_screenshot(page, f"nav_error_{safe_label}")
        return results

    for export_cfg in module_cfg["exports"]:
        selector  = export_cfg.get("selector")
        btn_text  = export_cfg.get("button_text", "Xuất")
        idx       = export_cfg.get("index", 0)
        rename_to = export_cfg.get("rename_to")

        try:
            # Resolve locator — CSS selector takes priority over text matching
            if selector:
                locator = page.locator(selector)
                desc    = f"selector='{selector}' index={idx}"
            else:
                locator = page.locator("button").filter(has_text=btn_text)
                desc    = f"button text='{btn_text}' index={idx}"

            # Wait for the element to be visible — handles SPA lazy rendering.
            # This replaces time.sleep(3) + instant count() check.
            try:
                locator.nth(idx).wait_for(state="visible", timeout=ELEMENT_TIMEOUT_MS)
            except Exception:
                logger.warning(
                    f"    Element not visible after {ELEMENT_TIMEOUT_MS}ms ({desc})"
                )
                save_screenshot(page, f"missing_element_{safe_label}_{idx}")
                continue

            logger.info(f"    Clicking export element ({desc})")

            with page.expect_download(timeout=60_000) as dl_info:
                locator.nth(idx).click()

            download  = dl_info.value
            suggested = download.suggested_filename or f"export_{hash_url(url)}_{idx}.xlsx"
            dest_path = os.path.join(DOWNLOAD_DIR, suggested)
            download.save_as(dest_path)

            logger.info(f"    Saved: {dest_path}")
            results[rename_to] = dest_path

            time.sleep(2)

        except Exception as e:
            logger.error(f"    Export failed (index {idx}): {e}")
            save_screenshot(page, f"export_error_{safe_label}_{idx}")

    return results


def run_rpa():
    """
    Phase 1: log in and download all exports.
    Returns a dict { rename_to_filename → downloaded_filepath }.
    """
    logger.info("=" * 60)
    logger.info("PHASE 1  —  RPA download")
    logger.info("=" * 60)

    download_map = {}  # rename_to → downloaded path

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=HEADLESS, slow_mo=0 if HEADLESS else 50)
        context = browser.new_context(accept_downloads=True)
        page    = context.new_page()

        # Wire up request/response capture
        def handle_request(req):
            if "/api/" not in req.url:
                return
            write_jsonl(REQUEST_LOG, {
                "timestamp": datetime.utcnow().isoformat(),
                "method":    req.method,
                "url":       req.url,
                "payload":   req.post_data,
            })

        def handle_response(resp):
            if "/api/" not in resp.url:
                return
            try:
                body = resp.json()
                fpath = os.path.join(CAPTURE_DIR, f"{hash_url(resp.url)}.json")
                with open(fpath, "w", encoding="utf8") as f:
                    json.dump(body, f, indent=2, ensure_ascii=False)
                write_jsonl(RESPONSE_LOG, {
                    "timestamp": datetime.utcnow().isoformat(),
                    "url":       resp.url,
                    "status":    resp.status,
                    "file":      fpath,
                })
                if "access_token" in body:
                    write_jsonl(TOKEN_LOG, body)
            except Exception:
                pass

        page.on("request",  handle_request)
        page.on("response", handle_response)

        try:
            rpa_login(page)
        except Exception as e:
            logger.error(f"Login failed: {e}")
            save_screenshot(page, "login_error")
            browser.close()
            return download_map

        MAX_MODULE_RETRIES = 2
        for module_cfg in MODULES:
            for attempt in range(1, MAX_MODULE_RETRIES + 1):
                try:
                    result = rpa_download_module(page, module_cfg)
                    download_map.update(result)
                    if result:
                        break  # at least one file downloaded — success
                    if attempt < MAX_MODULE_RETRIES:
                        logger.warning(
                            f"  Module yielded no downloads "
                            f"(attempt {attempt}/{MAX_MODULE_RETRIES}), retrying…"
                        )
                except Exception as e:
                    logger.error(
                        f"  Module error (attempt {attempt}/{MAX_MODULE_RETRIES}) "
                        f"({module_cfg['label']}): {e}"
                    )

        browser.close()

    logger.info(f"RPA phase complete. {len(download_map)} file(s) downloaded.")
    return download_map


# =============================================================================
# PHASE 2 — RENAME / MOVE FILES INTO RAW_DATA_DIR
# =============================================================================

def stage_files(download_map):
    """
    Move/copy each downloaded file to RAW_DATA_DIR with its expected name.
    """
    logger.info("=" * 60)
    logger.info("PHASE 2  —  Staging files to raw-data folder")
    logger.info("=" * 60)

    staged = []

    for rename_to, src_path in download_map.items():
        if not src_path or not os.path.exists(src_path):
            logger.warning(f"  Skipping missing file for '{rename_to}'")
            continue

        if rename_to is None:
            rename_to = os.path.basename(src_path)

        dest_path = os.path.join(RAW_DATA_DIR, rename_to)
        shutil.copy2(src_path, dest_path)
        logger.info(f"  Staged: {rename_to}")
        staged.append(rename_to)

    logger.info(f"Staging complete. {len(staged)} file(s) ready.")
    return staged


# =============================================================================
# PHASE 3 — INGEST EXCEL → DATABASE
# =============================================================================

def q(col):
    """Quote reserved SQL keywords."""
    return f'"{col}"' if col == "index" else col


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def read_excel(cfg):
    df = pd.read_excel(
        cfg["excel_file"],
        skiprows=cfg.get("skiprows", 0),
        header=None,
        skipfooter=cfg.get("skipfooter", 0),
        engine="openpyxl",
    )

    schema = cfg["schema"]
    df = df.iloc[:, :len(schema)]
    df.columns = schema
    df = df.dropna(how="all")

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
            max_len = df[col].astype(str).str.len().max()
            logger.debug(f"    {col}: max_len={max_len}")

    return df


def run_pipeline_cfg(cfg, conn):
    label  = cfg["label"]
    table  = cfg["table"]
    schema = cfg["schema"]
    mode   = cfg.get("mode", "upsert")

    excel_path = cfg["excel_file"]
    if not os.path.exists(excel_path):
        logger.warning(f"  [{label}] Excel not found, skipping: {excel_path}")
        return

    logger.info(f"  [{label}] mode={mode} table={table}")

    df = read_excel(cfg)
    logger.info(f"  [{label}] Rows from Excel: {len(df)}")

    cols_sql     = ", ".join(q(c) for c in schema)
    placeholders = ", ".join(["%s"] * len(schema))

    cur = conn.cursor()

    try:

        tbl = pgsql.Identifier(table)

        # ---- TRUNCATE + INSERT ----
        if mode == "truncate":
            cur.execute(pgsql.SQL("TRUNCATE TABLE {}").format(tbl))
            insert_sql = pgsql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                tbl,
                pgsql.SQL(cols_sql),
                pgsql.SQL(placeholders),
            )
            data = df.values.tolist()
            psycopg2.extras.execute_batch(cur, insert_sql, data, page_size=1000)
            conn.commit()
            logger.info(f"  [{label}] Inserted: {len(data)}")

        # ---- ON CONFLICT DO UPDATE ----
        elif mode == "conflict":
            key_cols  = cfg["key_cols"]
            non_key   = [c for c in schema if c not in key_cols]
            set_parts = [f"{q(c)} = EXCLUDED.{q(c)}" for c in non_key]
            if cfg.get("updated_at"):
                set_parts.append("updated_at = NOW()")

            conflict_cols = ", ".join(q(c) for c in key_cols)
            set_clause    = ", ".join(set_parts)

            insert_sql = pgsql.SQL("""
                INSERT INTO {} ({})
                VALUES ({})
                ON CONFLICT ({})
                DO UPDATE SET {}
            """).format(
                tbl,
                pgsql.SQL(cols_sql),
                pgsql.SQL(placeholders),
                pgsql.SQL(conflict_cols),
                pgsql.SQL(set_clause),
            )
            rows = [tuple(r) for r in df.itertuples(index=False)]
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=1000)
            conn.commit()
            logger.info(f"  [{label}] Upserted (conflict): {len(rows)}")

        # ---- UPSERT (key-based split) ----
        elif mode == "upsert":
            key_cols     = cfg["key_cols"]
            key_cols_sql = ", ".join(q(c) for c in key_cols)

            cur.execute(pgsql.SQL("SELECT {} FROM {}").format(
                pgsql.SQL(key_cols_sql), tbl
            ))
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

            insert_sql = pgsql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                tbl,
                pgsql.SQL(cols_sql),
                pgsql.SQL(placeholders),
            )
            if insert_rows:
                psycopg2.extras.execute_batch(cur, insert_sql, insert_rows, page_size=1000)

            non_key    = [c for c in schema if c not in key_cols]
            set_parts  = [f"{q(c)} = %s" for c in non_key]
            if cfg.get("updated_at"):
                set_parts.append("updated_at = NOW()")

            where_parts = [f"{q(c)} = %s" for c in key_cols]
            update_sql  = pgsql.SQL("UPDATE {} SET {} WHERE {}").format(
                tbl,
                pgsql.SQL(", ".join(set_parts)),
                pgsql.SQL(" AND ".join(where_parts)),
            )

            if update_rows:
                row_dicts     = [dict(zip(schema, r)) for r in update_rows]
                update_values = [
                    tuple(d[c] for c in non_key) + tuple(d[c] for c in key_cols)
                    for d in row_dicts
                ]
                psycopg2.extras.execute_batch(cur, update_sql, update_values, page_size=1000)

            conn.commit()
            logger.info(f"  [{label}] Inserted: {len(insert_rows)}  Updated: {len(update_rows)}")

    except Exception as e:
        conn.rollback()
        logger.error(f"  [{label}] DB error: {e}")
        raise

    finally:
        cur.close()


def run_ingest():
    logger.info("=" * 60)
    logger.info("PHASE 3  —  Ingest Excel → Database")
    logger.info("=" * 60)

    errors = []
    conn = get_connection()

    try:
        for cfg in PIPELINES:
            try:
                run_pipeline_cfg(cfg, conn)
            except Exception as e:
                errors.append((cfg["label"], str(e)))
    finally:
        conn.close()

    if errors:
        logger.error(f"Ingest completed with {len(errors)} error(s):")
        for label, msg in errors:
            logger.error(f"  {label}: {msg}")
    else:
        logger.info("Ingest complete — all pipelines succeeded.")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="MISA RPA + Ingest pipeline")
    parser.add_argument("--rpa-only",    action="store_true", help="Download only, skip ingest")
    parser.add_argument("--ingest-only", action="store_true", help="Skip RPA, run ingest only")
    args = parser.parse_args()

    start = datetime.now()
    logger.info(f"Pipeline started at {start.isoformat()}")

    if not args.ingest_only:
        download_map = run_rpa()
        if download_map and not args.rpa_only:
            stage_files(download_map)

    if not args.rpa_only:
        run_ingest()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"Pipeline finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
