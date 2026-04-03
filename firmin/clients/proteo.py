from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from firmin.utils.logger import get_logger

logger = get_logger(__name__)

PROTEO_URL = "https://firmin.proteoenterprise.co.uk/default.aspx"
FIND_ORDER_URL = "https://firmin.proteoenterprise.co.uk/groupage/findorder.aspx"


class ProteoClient:
    def __init__(self):
        self.username = os.getenv("PROTEO_USERNAME", "George")
        self.password = os.getenv("PROTEO_PASSWORD")
        if not self.password:
            raise RuntimeError("PROTEO_PASSWORD environment variable not set")

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
                # Login
                logger.debug("Proteo: logging in for job %s", job_number)
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

                    // Prefer a row with an order link
                    for (const row of rows) {
                        if (row.querySelector('td a[id*="hypUpdateOrder"]')) {
                            dataRow = row;
                            break;
                        }
                    }
                    // Fallback: first tr with tds but no ths
                    if (!dataRow) {
                        for (const row of rows) {
                            if (row.querySelector('td') && !row.querySelector('th')) {
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
                        order_number:          getText(13),
                        po_number:             getText(13),
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

                if not row_data or not row_data.get("order_id"):
                    logger.warning("Proteo: job %s not found in results", job_number)
                    return None

                row_data["processed_at"] = datetime.now(timezone.utc).isoformat()
                logger.debug("Proteo: extracted job %s -> order_id=%s", job_number, row_data["order_id"])
                return row_data

            except Exception as e:
                logger.error("Proteo scrape failed for job %s: %s", job_number, e)
                return None
            finally:
                browser.close()
