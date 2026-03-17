# MISA RPA + Ingest Pipeline

End-to-end automation: log in to MISA, download Excel exports, and load them into PostgreSQL.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Environment Variables](#environment-variables)
5. [Configuration](#configuration)
   - [MODULES — Download targets](#modules--download-targets)
   - [PIPELINES — Ingest config](#pipelines--ingest-config)
6. [Usage](#usage)
7. [Pipeline Phases](#pipeline-phases)
   - [Phase 1 — RPA Download](#phase-1--rpa-download)
   - [Phase 2 — Stage Files](#phase-2--stage-files)
   - [Phase 3 — Ingest to Database](#phase-3--ingest-to-database)
8. [Ingest Modes](#ingest-modes)
9. [Directory Layout](#directory-layout)
10. [Logs and Artifacts](#logs-and-artifacts)
11. [Troubleshooting](#troubleshooting)
12. [File Map](#file-map)

---

## Overview

`rpa_pipeline_full.py` is a single-script pipeline that:

1. **RPA** — Launches a Chromium browser via Playwright, logs into [actapp.misa.vn](https://actapp.misa.vn), handles OTP verification, and downloads Excel reports for each configured module.
2. **Stage** — Renames and copies each downloaded file into the raw-data folder with the exact filename the ingest layer expects.
3. **Ingest** — Reads every Excel file with pandas and bulk-loads the rows into PostgreSQL using three configurable strategies: `upsert`, `conflict`, or `truncate`.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                  rpa_pipeline_full.py                    │
│                                                          │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────┐  │
│  │  Phase 1    │   │   Phase 2    │   │   Phase 3    │  │
│  │  RPA        │──▶│   Stage      │──▶│   Ingest     │  │
│  │  (Playwright│   │   (rename /  │   │   (pandas +  │  │
│  │   + IMAP)   │   │    copy)     │   │   psycopg2)  │  │
│  └─────────────┘   └──────────────┘   └──────────────┘  │
│        │                  │                   │          │
│        ▼                  ▼                   ▼          │
│   misa_reports/    raw-data/*.xlsx      PostgreSQL DB    │
└──────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Dependency | Version installed |
|---|---|
| Python 3.9+ | — |
| playwright | 1.58.0 |
| pandas | 3.0.1 |
| openpyxl | 3.1.5 |
| psycopg2-binary | 2.9.11 |
| python-dotenv | 1.2.2 |

### Setup (venv — recommended)

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
playwright install chromium
```

---

## Environment Variables

Create a `.env` file in the project root:

```dotenv
# MISA login
MISA_USERNAME=your_misa_email@example.com
MISA_PASSWORD=your_misa_password

# OTP email (Gmail recommended)
OTP_EMAIL=your_otp_email@gmail.com
OTP_PASSWORD=your_gmail_app_password
IMAP_SERVER=imap.gmail.com        # optional, defaults to imap.gmail.com

# PostgreSQL
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# Optional overrides
RAW_DATA_DIR=C:\Projects\ADG\DWH\raw-data   # defaults to ./raw-data
HEADLESS=false                               # set true for server/CI runs
```

> **Gmail OTP**: use an [App Password](https://support.google.com/accounts/answer/185833), not your account password.

---

## Configuration

### MODULES — Download targets

Defined in the `MODULES` list. Each entry maps one MISA module URL to one or more Excel export buttons.

```python
{
    "path":   "/app/item",           # URL path appended to BASE_URL
    "label":  "Products / Items",   # human-readable label for logs
    "exports": [
        {
            "button_text": "Xuất",   # text on the export button
            "index":       0,        # 0-based index if multiple buttons match
            "rename_to":   "Danh_sach_hang_hoa_dich_vu.xlsx",  # target filename
        },
    ],
}
```

| Status | Full URL | Label | Selector | Exports to |
|---|---|---|---|---|
| Confirmed | `/app/PU/PUVendor` | Suppliers | `div.mi-excel__nav` | `Danh_sach_nha_cung_cap.xlsx` |
| Confirmed | `/app/SA/SAInventoryItems` | Products / Items | `button.ms-button-feature` | `Danh_sach_hang_hoa_dich_vu.xlsx` |
| Confirmed | `/app/DI/DIStock` | Warehouses | `div.mi-excel__nav` | `Danh_sach_kho.xlsx` |
| TODO | `/app/account-object` | Customers | `div.mi-excel__nav` *(assumed)* | `Danh_sach_khach_hang.xlsx` |
| TODO | `/app/inventory` | Stock Remaining | `div.mi-excel__nav` *(assumed)* | `stock_remaining.xlsx` |
| TODO | `/app/voucher` | Stock In | `div.mi-excel__nav` *(assumed)* | `stock_in.xlsx` |
| TODO | `/app/invoice` | Stock Out | `div.mi-excel__nav` *(assumed)* | `stock_out.xlsx` |

**Selector resolution order** (per export entry in `MODULES`):
1. `selector` key → CSS selector clicked directly (e.g. `div.mi-excel__nav`, `button.ms-button-feature`)
2. `button_text` key → fallback, finds `<button>` by visible text (e.g. `"Xuất"`)

> **Finding remaining URLs/selectors**: open the module in Chrome DevTools → right-click the Excel export icon → *Inspect* → copy the element's class. Then copy the URL from the address bar and update the `MODULES` entry.

---

### PIPELINES — Ingest config

Defined in the `PIPELINES` list. Each entry describes one Excel → database table mapping.

```python
{
    "label":      "dim_customer",                     # pipeline name (logs)
    "excel_file": r"C:\...\raw-data\Danh_sach_khach_hang.xlsx",
    "skiprows":   3,       # header rows to skip at the top
    "skipfooter": 1,       # summary rows to skip at the bottom
    "schema":     ["index", "customer_code", ...],   # column names in order
    "table":      "dim_customer",                    # target DB table
    "key_cols":   ["customer_code"],                 # primary-key columns
    "mode":       "upsert",   # upsert | conflict | truncate
    "updated_at": True,       # append updated_at = NOW() on update
    "preprocess": "stock_in", # optional named pre-processing step
}
```

#### Configured pipelines

| Label | Source file | Table | Mode |
|---|---|---|---|
| `dim_measurement_unit` | `Danh_sach_don_vi_tinh.xlsx` | `dim_measurement_unit` | conflict |
| `dim_storage` | `Danh_sach_kho.xlsx` | `dim_storage` | upsert |
| `dim_storage_group` | `storage_group.xlsx` | `dim_storage_group` | upsert |
| `dim_storage_group_category` | `storage_group_category.xlsx` | `dim_storage_group_category` | upsert |
| `dim_supplier` | `Danh_sach_nha_cung_cap.xlsx` | `dim_supplier` | upsert |
| `dim_customer` | `Danh_sach_khach_hang.xlsx` | `dim_customer` | upsert |
| `dim_product` | `Danh_sach_hang_hoa_dich_vu.xlsx` | `dim_product` | upsert |
| `stock_in` | `stock_in.xlsx` | `stock_in` | upsert |
| `stock_out` | `stock_out.xlsx` | `stock_out` | truncate |
| `stock_remaining` | `stock_remaining.xlsx` | `stock_remaining` | truncate |

---

## Usage

```bash
# Activate venv first
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS / Linux

# Full pipeline — RPA download → stage → ingest
python rpa_pipeline_full.py

# Download only (skip DB ingest)
python rpa_pipeline_full.py --rpa-only

# Skip RPA, ingest existing Excel files only
python rpa_pipeline_full.py --ingest-only
```

---

## Pipeline Phases

### Phase 1 — RPA Download

```
Login  ──▶  OTP (if prompted)  ──▶  For each module:
                                       navigate → click "Xuất" → save file
```

1. Launches Chromium (headless controlled by `HEADLESS` env var — default `false`).
2. Fills username and password, clicks **Đăng nhập**.
3. If an OTP input appears, polls the inbox via IMAP for an unread email containing a 6-digit code, fills it, and clicks **Xác nhận**.
4. For each module in `MODULES`, navigates to the URL and waits up to `ELEMENT_TIMEOUT_MS` (15 s) for the export button to become visible — handles MISA's SPA async rendering.
5. If the element is still not found, saves a `missing_element_<label>.png` screenshot and retries the module (up to 2 attempts) before moving on.
6. Saves each downloaded `.xlsx` to `misa_reports/`.
7. All `/api/` requests and responses are logged to `misa_api_capture/`.

### Phase 2 — Stage Files

Copies each downloaded file from `misa_reports/` to `C:\Projects\ADG\DWH\raw-data\` with the `rename_to` filename. Files not found in the download map are skipped with a warning.

### Phase 3 — Ingest to Database

For each pipeline config:

1. Checks the Excel file exists — skips with a warning if not.
2. Reads the file with `pandas.read_excel` (skips header/footer rows, assigns schema column names).
3. Applies optional pre-processing (date coercion, row filtering).
4. Connects to PostgreSQL and runs the configured ingest mode.
5. Commits and closes the connection; rolls back on error.

---

## Ingest Modes

### `truncate`

```sql
TRUNCATE TABLE <table>;
INSERT INTO <table> (...) VALUES (...);
```

Full reload — fastest for tables that are completely replaced each run (e.g. `stock_out`, `stock_remaining`).

---

### `conflict`

```sql
INSERT INTO <table> (...)
VALUES (...)
ON CONFLICT (<key_cols>)
DO UPDATE SET col1 = EXCLUDED.col1, ..., updated_at = NOW();
```

Single-pass upsert using a PostgreSQL unique constraint. Requires a `UNIQUE` index on `key_cols` in the database.

---

### `upsert`

```
SELECT existing keys  ──▶  split rows into INSERT batch + UPDATE batch
    INSERT new rows
    UPDATE existing rows  (SET non-key cols WHERE key = ...)
```

Two-pass upsert that works without a unique index. Slightly slower than `conflict` but more portable.

---

## Directory Layout

```
misa-pipeline/
├── rpa_pipeline_full.py      ← main script (this file)
├── rpa_misa_pipeline.py      ← original RPA-only script
├── pipeline.py               ← original ingest-only script
├── .env                      ← credentials (never commit)
├── .gitignore
│
├── misa_reports/             ← raw downloads from MISA
│   └── *.xlsx
│
├── misa_api_capture/         ← API request/response logs
│   ├── requests.jsonl
│   ├── responses.jsonl
│   ├── tokens.jsonl
│   └── <md5>.json
│
├── logs/
│   ├── misa_rpa.log          ← combined run log
│   └── *.png                 ← error screenshots
│
└── backup/                   ← legacy per-table scripts
    └── upsert_*.py

C:\Projects\ADG\DWH\raw-data\ ← staged Excel files read by ingest
    ├── Danh_sach_don_vi_tinh.xlsx      (manual — not downloaded by RPA)
    ├── Danh_sach_kho.xlsx
    ├── Danh_sach_nha_cung_cap.xlsx
    ├── Danh_sach_khach_hang.xlsx
    ├── Danh_sach_hang_hoa_dich_vu.xlsx
    ├── storage_group.xlsx              (manual — not downloaded by RPA)
    ├── storage_group_category.xlsx     (manual — not downloaded by RPA)
    ├── stock_in.xlsx
    ├── stock_out.xlsx
    └── stock_remaining.xlsx
```

---

## Logs and Artifacts

| Path | Content |
|---|---|
| `logs/misa_rpa.log` | Timestamped INFO/ERROR log for the full run |
| `logs/*.png` | Screenshots captured on navigation or export errors |
| `misa_api_capture/requests.jsonl` | Every `/api/` request (method, URL, payload) |
| `misa_api_capture/responses.jsonl` | Every `/api/` response (status, URL, saved file path) |
| `misa_api_capture/tokens.jsonl` | Any `access_token` payloads detected in responses |
| `misa_api_capture/<md5>.json` | Full JSON body of each unique API response |

---

## Troubleshooting

### OTP never received

- Confirm `OTP_EMAIL` and `OTP_PASSWORD` in `.env` are correct.
- For Gmail, use an **App Password** (not your account password). Enable 2FA first, then generate one at `myaccount.google.com/apppasswords`.
- Increase the timeout in `get_latest_otp(timeout=120)` if mail delivery is slow.

### Export button not found

- MISA's UI may render "Xuất khẩu" instead of "Xuất". Update `button_text` in `MODULES`.
- If multiple buttons match, check the `index` value — try `0`, `1`, `2` …
- An error screenshot is saved to `logs/` automatically.

### Download times out

- Increase `timeout=60_000` (ms) in `page.expect_download(timeout=...)`.
- Large reports can take 30–90 seconds to generate server-side.

### DB connection fails

- Verify all `DB_*` variables in `.env`.
- Check that the PostgreSQL host is reachable from the machine running the script.

### `Danh_sach_don_vi_tinh.xlsx` / `storage_group*.xlsx` not found

These files are **not exported by MISA RPA** — they must be placed in `raw-data/` manually. The ingest pipeline skips them with a warning if missing.

### Running headless (on a server)

In `run_rpa()`, change:

```python
browser = p.chromium.launch(headless=False, slow_mo=50)
# →
browser = p.chromium.launch(headless=True, slow_mo=0)
```

---

## File Map

| File | Role |
|---|---|
| `rpa_pipeline_full.py` | Combined RPA + ingest pipeline (main entry point) |
| `rpa_misa_pipeline.py` | Original RPA-only script (download + API capture) |
| `pipeline.py` | Original ingest-only script (Excel → PostgreSQL) |
| `backup/run_pipeline.py` | Legacy subprocess runner for individual upsert scripts |
| `backup/upsert_*.py` | Legacy per-table ingest scripts (superseded by `pipeline.py`) |
