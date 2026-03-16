import os
import json
import hashlib
import time
import re
import imaplib
import email
import logging
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# =========================
# LOAD ENV
# =========================

load_dotenv()

MISA_USERNAME = os.getenv("MISA_USERNAME")
MISA_PASSWORD = os.getenv("MISA_PASSWORD")

EMAIL_USER = os.getenv("OTP_EMAIL")
EMAIL_PASS = os.getenv("OTP_PASSWORD")
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")

BASE_URL = "https://actapp.misa.vn"

MODULES = [
    "/app/account-object",
    "/app/item",
    "/app/warehouse",
    "/app/voucher",
    "/app/invoice",
    "/app/inventory"
]

# =========================
# DIRECTORIES
# =========================

OUTPUT_DIR = "misa_api_capture"
DOWNLOAD_DIR = "misa_reports"
LOG_DIR = "logs"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

REQUEST_LOG = os.path.join(OUTPUT_DIR, "requests.jsonl")
RESPONSE_LOG = os.path.join(OUTPUT_DIR, "responses.jsonl")
TOKEN_LOG = os.path.join(OUTPUT_DIR, "tokens.jsonl")

# =========================
# LOGGING
# =========================

LOG_FILE = os.path.join(LOG_DIR, "misa_rpa.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

# =========================
# UTILITIES
# =========================

def write_jsonl(path, data):

    with open(path, "a", encoding="utf8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


# =========================
# OTP READER
# =========================

def get_latest_otp(timeout=120):

    logger.info("Waiting for OTP email")

    start = time.time()

    while time.time() - start < timeout:

        try:

            mail = imaplib.IMAP4_SSL(IMAP_SERVER)
            mail.login(EMAIL_USER, EMAIL_PASS)
            mail.select("inbox")

            status, messages = mail.search(None, '(UNSEEN)')

            logger.debug(f"Unread emails: {messages}")

            for num in messages[0].split():

                status, data = mail.fetch(num, "(RFC822)")
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

                    logger.info(f"OTP detected: {code}")

                    return code

        except Exception as e:

            logger.error(f"OTP read error: {e}")

        time.sleep(5)

    raise Exception("OTP not received")


# =========================
# SCREENSHOT HELPER
# =========================

def save_screenshot(page, name):

    path = os.path.join(LOG_DIR, f"{name}.png")

    page.screenshot(path=path)

    logger.info(f"Screenshot saved: {path}")


# =========================
# MAIN PIPELINE
# =========================

def run():

    logger.info("Starting MISA RPA pipeline")

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=False,
            slow_mo=50
        )

        context = browser.new_context(
            accept_downloads=True
        )

        page = context.new_page()

        # =========================
        # CAPTURE REQUEST
        # =========================

        def handle_request(request):

            if "/api/" not in request.url:
                return

            payload = request.post_data

            data = {
                "timestamp": datetime.utcnow().isoformat(),
                "method": request.method,
                "url": request.url,
                "payload": payload
            }

            write_jsonl(REQUEST_LOG, data)

            logger.debug(f"API REQUEST {request.method} {request.url}")

        page.on("request", handle_request)

        # =========================
        # CAPTURE RESPONSE
        # =========================

        def handle_response(response):

            if "/api/" not in response.url:
                return

            try:

                body = response.json()

                file_hash = hash_url(response.url)

                filepath = os.path.join(
                    OUTPUT_DIR,
                    f"{file_hash}.json"
                )

                with open(filepath, "w", encoding="utf8") as f:
                    json.dump(body, f, indent=2, ensure_ascii=False)

                data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "url": response.url,
                    "status": response.status,
                    "file": filepath
                }

                write_jsonl(RESPONSE_LOG, data)

                logger.debug(f"API RESPONSE {response.status} {response.url}")

                # detect token
                if "access_token" in body:

                    logger.info("Access token detected")

                    write_jsonl(TOKEN_LOG, body)

            except Exception as e:

                logger.warning(f"Failed to parse response {response.url} | {e}")

        page.on("response", handle_response)

        # =========================
        # LOGIN
        # =========================

        try:

            logger.info("Opening login page")

            page.goto(BASE_URL)

            page.wait_for_load_state("networkidle")

            username = page.locator('input[type="text"]').first
            username.wait_for()
            username.fill(MISA_USERNAME)

            password = page.locator('input[type="password"]')
            password.wait_for()
            password.fill(MISA_PASSWORD)

            logger.info("Submitting credentials")

            page.locator("button").filter(has_text="Đăng nhập").click()

            time.sleep(5)

            # =========================
            # OTP STEP
            # =========================

            try:

                otp_input = page.locator('input[type="text"]').nth(1)

                if otp_input.is_visible():

                    otp = get_latest_otp()

                    otp_input.fill(otp)

                    page.locator("button").filter(has_text="Xác nhận").click()

                    page.wait_for_load_state("networkidle")

                    logger.info("OTP verification successful")

            except Exception as e:

                logger.warning(f"No OTP required or OTP step skipped | {e}")

            logger.info("Login successful")

        except Exception as e:

            logger.error(f"Login failed | {e}")

            save_screenshot(page, "login_error")

            browser.close()

            return

        # =========================
        # MODULE SCAN + EXPORT
        # =========================

        for module in MODULES:

            url = BASE_URL + module

            logger.info(f"Navigating module: {module}")

            try:

                page.goto(url)

                page.wait_for_load_state("networkidle")

                time.sleep(5)

                export_button = page.locator("button").filter(has_text="Xuất")

                if export_button.count() > 0:

                    logger.info("Export button detected")

                    with page.expect_download(timeout=30000) as download_info:

                        export_button.first.click()

                    download = download_info.value

                    filepath = os.path.join(
                        DOWNLOAD_DIR,
                        download.suggested_filename
                    )

                    download.save_as(filepath)

                    logger.info(f"Report downloaded: {filepath}")

                else:

                    logger.warning("Export button not found")

            except Exception as e:

                logger.error(f"Module failed: {module} | {e}")

                save_screenshot(page, f"module_error_{module.replace('/','_')}")

        logger.info("Pipeline completed")

        browser.close()


# =========================
# ENTRY
# =========================

if __name__ == "__main__":

    run()