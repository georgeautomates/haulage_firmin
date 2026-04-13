from __future__ import annotations
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

PROTEO_URL     = "https://firmin.proteoenterprise.co.uk/default.aspx"
FIND_ORDER_URL = "https://firmin.proteoenterprise.co.uk/groupage/findorder.aspx"
ADD_ORDER_URL  = "https://firmin.proteoenterprise.co.uk/groupage/AddOrder.aspx?csid=Wt6g58"

# Business type select values (from inspection)
BUSINESS_TYPE_VALUES = {
    "artic reloads": "5",
    "distribution | rigids": "2",
    "general | part/full load": "1",
    "full load": "1",
}


@dataclass
class RpaEntryResult:
    """Result of a Proteo RPA order entry attempt."""
    job_number: str
    success: bool
    screenshot_url: str = ""          # Drive URL of the screenshot
    # Values as actually typed into the form (read back after filling)
    typed_client: str = ""
    typed_collection_point: str = ""
    typed_delivery_point: str = ""
    typed_collection_date: str = ""
    typed_collection_time: str = ""
    typed_delivery_date: str = ""
    typed_delivery_time: str = ""
    typed_order_number: str = ""
    typed_price: str = ""
    # Per-field agreement vs the extraction
    field_matches: dict = field(default_factory=dict)
    agreement_score: int = 0           # 0-100
    error: str = ""


class ProteoClient:
    def __init__(self):
        self.username = os.getenv("PROTEO_USERNAME", "George")
        self.password = os.getenv("PROTEO_PASSWORD")
        if not self.password:
            raise RuntimeError("PROTEO_PASSWORD environment variable not set")

    def _login(self, page):
        """Log in to Proteo. Raises on failure."""
        page.goto(PROTEO_URL, wait_until="networkidle")
        page.fill('input[name="txtUserName"]', self.username)
        page.fill('input[name="txtPIN"]', self.password)
        page.click('input[name="btnLogon"]')
        page.wait_for_load_state("networkidle")
        # Dismiss notification popup if present
        for selector in ['button:has-text("Block")', 'button:has-text("No Thanks")']:
            try:
                btn = page.locator(selector)
                if btn.is_visible(timeout=2000):
                    btn.click()
                    break
            except PlaywrightTimeout:
                pass

    def enter_order(self, order: dict, drive_client=None) -> RpaEntryResult:
        """
        Fill the Proteo AddOrder form with extraction data but do NOT save.
        Takes a screenshot, uploads to Drive, reads back typed values,
        and compares them against the extraction.

        Args:
            order: dict with extraction fields (collection_point, delivery_point,
                   collection_date, collection_time, delivery_date, delivery_time,
                   order_number, price, business_type, service, pallets, spaces,
                   client_name, job_number / delivery_order_number)
            drive_client: DriveClient instance for screenshot upload (optional)

        Returns RpaEntryResult.
        """
        job_number = order.get("delivery_order_number") or order.get("job_number", "unknown")

        def _parse_date(val: str) -> str:
            """Convert DD/MM/YYYY → DD/MM/YY for Proteo's dateInput format."""
            if not val:
                return ""
            # Already short
            if re.match(r"\d{2}/\d{2}/\d{2}$", val):
                return val
            # DD/MM/YYYY → DD/MM/YY
            m = re.match(r"(\d{2})/(\d{2})/(\d{4})$", val)
            if m:
                return f"{m.group(1)}/{m.group(2)}/{m.group(3)[2:]}"
            return val

        def _strip_currency(val: str) -> str:
            """£300.00 → 300.00"""
            return val.lstrip("£").strip() if val else ""

        result = RpaEntryResult(job_number=job_number, success=False)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                ignore_https_errors=True,
            )
            context.set_default_timeout(60000)
            page = context.new_page()

            try:
                logger.info("RPA: logging in for job %s", job_number)
                self._login(page)

                page.goto(ADD_ORDER_URL, wait_until="networkidle")
                page.wait_for_timeout(1000)

                # ── Client ──────────────────────────────────────────────────
                client_name = order.get("client_name", "")
                if client_name:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_cboClient_Input", client_name)
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(800)

                # ── Business Type (select) ───────────────────────────────────
                bt_raw = order.get("business_type", "").lower()
                bt_val = BUSINESS_TYPE_VALUES.get(bt_raw, "5")  # default Artic Reloads
                page.select_option(
                    "#ctl00_ContentPlaceHolder1_ucOrder_cboBusinessType",
                    value=bt_val,
                )
                page.wait_for_timeout(500)

                # ── Service ──────────────────────────────────────────────────
                service = order.get("service", "")
                if service:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_cboService_Input", service)
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(500)

                # ── Order / Load numbers ──────────────────────────────────────
                order_number = order.get("order_number", "")
                if order_number:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_txtLoadNumber", order_number)

                delivery_order_number = order.get("delivery_order_number", "")
                if delivery_order_number:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_txtDeliveryOrderNumber", delivery_order_number)

                # ── Pallets / Spaces ─────────────────────────────────────────
                pallets = str(order.get("pallets", ""))
                if pallets:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_rntxtPallets", pallets)
                    page.keyboard.press("Tab")

                spaces = str(order.get("spaces", ""))
                if spaces:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_rntxtPalletSpaces", spaces)
                    page.keyboard.press("Tab")

                # ── Collection Point (autocomplete combobox) ─────────────────
                collection_point = order.get("collection_point", "")
                if collection_point:
                    page.fill(
                        "#ctl00_ContentPlaceHolder1_ucOrder_ucCollectionPoint_cboPoint_Input",
                        collection_point,
                    )
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(800)

                # ── Collection date / time ────────────────────────────────────
                col_date = _parse_date(order.get("collection_date", ""))
                if col_date:
                    page.fill(
                        "#ctl00_ContentPlaceHolder1_ucOrder_dteCollectionFromDate_dateInput",
                        col_date,
                    )
                    page.keyboard.press("Tab")

                col_time = order.get("collection_time", "")
                if col_time:
                    page.fill(
                        "#ctl00_ContentPlaceHolder1_ucOrder_dteCollectionFromTime_dateInput",
                        col_time,
                    )
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(300)

                # ── Delivery Point ────────────────────────────────────────────
                delivery_point = order.get("delivery_point", "")
                if delivery_point:
                    page.fill(
                        "#ctl00_ContentPlaceHolder1_ucOrder_ucDeliveryPoint_cboPoint_Input",
                        delivery_point,
                    )
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(800)

                # ── Delivery date / time ──────────────────────────────────────
                del_date = _parse_date(order.get("delivery_date", ""))
                if del_date:
                    page.fill(
                        "#ctl00_ContentPlaceHolder1_ucOrder_dteDeliveryFromDate_dateInput",
                        del_date,
                    )
                    page.keyboard.press("Tab")

                del_time = order.get("delivery_time", "")
                if del_time:
                    page.fill(
                        "#ctl00_ContentPlaceHolder1_ucOrder_dteDeliveryFromTime_dateInput",
                        del_time,
                    )
                    page.keyboard.press("Tab")
                    page.wait_for_timeout(300)

                # ── Rate ──────────────────────────────────────────────────────
                price = _strip_currency(order.get("price", "") or order.get("rate", ""))
                if price:
                    page.fill("#ctl00_ContentPlaceHolder1_ucOrder_rntOrderRate", price)
                    page.keyboard.press("Tab")

                # ── Notes — stamp job number so form is identifiable ──────────
                page.fill(
                    "#ctl00_ContentPlaceHolder1_ucOrder_txtTrafficNotes",
                    f"RPA dry-run — job {job_number} — DO NOT SAVE",
                )

                page.wait_for_timeout(1000)

                # ── Screenshot ────────────────────────────────────────────────
                screenshot_path = f"/tmp/rpa_{job_number}.png"
                page.screenshot(path=screenshot_path, full_page=True)
                logger.info("RPA: screenshot saved to %s", screenshot_path)

                # ── Read back typed values ────────────────────────────────────
                def read(selector: str) -> str:
                    try:
                        return page.input_value(selector).strip()
                    except Exception:
                        return ""

                typed_collection = read(
                    "#ctl00_ContentPlaceHolder1_ucOrder_ucCollectionPoint_cboPoint_Input"
                )
                typed_delivery = read(
                    "#ctl00_ContentPlaceHolder1_ucOrder_ucDeliveryPoint_cboPoint_Input"
                )
                typed_col_date = read(
                    "#ctl00_ContentPlaceHolder1_ucOrder_dteCollectionFromDate_dateInput"
                )
                typed_col_time = read(
                    "#ctl00_ContentPlaceHolder1_ucOrder_dteCollectionFromTime_dateInput"
                )
                typed_del_date = read(
                    "#ctl00_ContentPlaceHolder1_ucOrder_dteDeliveryFromDate_dateInput"
                )
                typed_del_time = read(
                    "#ctl00_ContentPlaceHolder1_ucOrder_dteDeliveryFromTime_dateInput"
                )
                typed_order = read("#ctl00_ContentPlaceHolder1_ucOrder_txtLoadNumber")
                typed_price = read("#ctl00_ContentPlaceHolder1_ucOrder_rntOrderRate")
                typed_client = read("#ctl00_ContentPlaceHolder1_ucOrder_cboClient_Input")

                result.typed_client = typed_client
                result.typed_collection_point = typed_collection
                result.typed_delivery_point = typed_delivery
                result.typed_collection_date = typed_col_date
                result.typed_collection_time = typed_col_time
                result.typed_delivery_date = typed_del_date
                result.typed_delivery_time = typed_del_time
                result.typed_order_number = typed_order
                result.typed_price = typed_price

                # ── Field-level comparison ────────────────────────────────────
                def _norm(s: str) -> str:
                    return s.strip().lower()

                checks = {
                    "collection_point": (_norm(typed_collection), _norm(collection_point)),
                    "delivery_point":   (_norm(typed_delivery),   _norm(delivery_point)),
                    "collection_date":  (_norm(typed_col_date),   _norm(col_date)),
                    "collection_time":  (_norm(typed_col_time),   _norm(col_time)),
                    "delivery_date":    (_norm(typed_del_date),   _norm(del_date)),
                    "delivery_time":    (_norm(typed_del_time),   _norm(del_time)),
                    "order_number":     (_norm(typed_order),      _norm(order_number)),
                    "price":            (_norm(typed_price),      _norm(price)),
                }
                field_matches = {k: (v[0] == v[1]) for k, v in checks.items() if v[1]}
                result.field_matches = field_matches
                agreed = sum(1 for v in field_matches.values() if v)
                result.agreement_score = round(agreed / len(field_matches) * 100) if field_matches else 0

                result.success = True
                logger.info(
                    "RPA entry done for job %s — agreement %d%% (%d/%d fields match)",
                    job_number, result.agreement_score, agreed, len(field_matches),
                )

                # ── Upload screenshot to Drive ────────────────────────────────
                if drive_client:
                    try:
                        with open(screenshot_path, "rb") as f:
                            screenshot_bytes = f.read()
                        result.screenshot_url = drive_client.upload_pdf(
                            pdf_bytes=screenshot_bytes,
                            filename=f"rpa_{job_number}.png",
                        )
                        logger.info("RPA screenshot uploaded: %s", result.screenshot_url)
                    except Exception as e:
                        logger.warning("RPA screenshot upload failed for job %s: %s", job_number, e)

            except Exception as e:
                result.error = str(e)
                logger.error("RPA entry failed for job %s: %s", job_number, e, exc_info=True)
                # Still try to take screenshot for debugging
                try:
                    page.screenshot(path=f"/tmp/rpa_{job_number}_error.png")
                except Exception:
                    pass
            finally:
                browser.close()

        return result

    def scrape_job(self, job_number: str) -> Optional[dict]:
        """
        Log in to Proteo TMS, search for job_number, extract the order row.
        Returns a dict matching Verification sheet columns, or None if not found.
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                ignore_https_errors=True,
            )
            context.set_default_timeout(60000)
            page = context.new_page()

            try:
                logger.debug("Proteo: logging in for job %s", job_number)
                self._login(page)

                # Navigate to Find Order
                page.goto(FIND_ORDER_URL, wait_until="networkidle")

                # Search
                page.fill('input[id="ctl00_txtSearchString"]', job_number)
                page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle")

                # Wait for results table
                try:
                    page.wait_for_selector("table", timeout=10000)
                except PlaywrightTimeout:
                    logger.warning("Proteo: no results table for job %s", job_number)
                    return None

                # Extract first data row
                row_data = page.evaluate("""() => {
                    const rows = document.querySelectorAll('table tr');
                    let dataRow = null;

                    // Prefer a row with an order link (most reliable)
                    for (const row of rows) {
                        if (row.querySelector('td a[id*="hypUpdateOrder"]')) {
                            dataRow = row;
                            break;
                        }
                    }
                    // Fallback: first tr where first cell looks like a numeric order ID
                    if (!dataRow) {
                        for (const row of rows) {
                            const firstCell = row.querySelector('td');
                            if (firstCell && /^\\d+$/.test(firstCell.textContent.trim())) {
                                dataRow = row;
                                break;
                            }
                        }
                    }

                    if (!dataRow) return null;

                    const cells = dataRow.querySelectorAll('td');
                    const getText = (i) => cells[i]?.textContent?.trim() || '';

                    const collectAt = getText(15);
                    const deliverAt = getText(18);

                    return {
                        order_id:              getText(0),
                        client_name:           getText(1),
                        run_id:                getText(3),
                        business_type:         getText(4),
                        rate:                  getText(6),
                        pallets:               getText(9),
                        spaces:                getText(10),
                        weight:                getText(11),
                        service:               getText(12),
                        order_number:          getText(13) ? getText(13).split(/\\s+/)[0] : '',
                        po_number:             getText(13) ? getText(13).split(/\\s+/)[0] : '',
                        collection_point:      getText(14),
                        collection_date:       collectAt.split('\\n')[0] || '',
                        collection_time:       collectAt.split('\\n')[1] || '',
                        delivery_point:        getText(16),
                        delivery_postcode:     getText(17),
                        delivery_date:         deliverAt.split('\\n')[0] || '',
                        delivery_time:         deliverAt.split('\\n')[1] || '',
                        delivery_order_number: getText(19),
                        goods_type:            getText(21),
                    };
                }""")

                if not row_data or not str(row_data.get("order_id", "")).isdigit() or len(str(row_data.get("order_id", ""))) < 5:
                    logger.warning("Proteo: job %s not found in results (order_id=%s)", job_number, row_data.get("order_id") if row_data else None)
                    return None

                # Validate the result belongs to the right client — Proteo search is
                # global across all clients, so a matching job number from Pallet Track
                # or another company can be returned instead of the DS Smith job.
                client = str(row_data.get("client_name", "")).lower()
                if not any(kw in client for kw in ("st regis", "ds smith", "fibre", "reels", "unipet")):
                    logger.warning(
                        "Proteo: job %s result rejected — client_name '%s' does not match DS Smith/St Regis",
                        job_number, row_data.get("client_name"),
                    )
                    return None

                row_data["processed_at"] = datetime.now(timezone.utc).isoformat()
                logger.debug("Proteo: extracted job %s -> order_id=%s client=%s", job_number, row_data["order_id"], row_data["client_name"])
                return row_data

            except Exception as e:
                logger.error("Proteo scrape failed for job %s: %s", job_number, e)
                return None
            finally:
                browser.close()
