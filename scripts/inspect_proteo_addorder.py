"""
One-shot script to inspect the Proteo AddOrder form fields.
Run this on the VPS where PROTEO_PASSWORD is set, then paste the output back.

Usage:
    cd /opt/firmin
    .venv/bin/python scripts/inspect_proteo_addorder.py
"""
from __future__ import annotations
import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from playwright.sync_api import sync_playwright

PROTEO_URL    = "https://firmin.proteoenterprise.co.uk/default.aspx"
ADD_ORDER_URL = "https://firmin.proteoenterprise.co.uk/groupage/AddOrder.aspx?csid=Wt6g58"

username = os.getenv("PROTEO_USERNAME", "George")
password = os.getenv("PROTEO_PASSWORD")
if not password:
    print("ERROR: PROTEO_PASSWORD not set"); sys.exit(1)

print(f"Logging in as {username}...")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        ignore_https_errors=True,
    )
    context.set_default_timeout(60000)
    page = context.new_page()

    # Login
    page.goto(PROTEO_URL, wait_until="networkidle")
    page.fill('input[name="txtUserName"]', username)
    page.fill('input[name="txtPIN"]', password)
    page.click('input[name="btnLogon"]')
    page.wait_for_load_state("networkidle")
    print("Logged in. URL:", page.url)

    # Dismiss notification popup if present
    for selector in ['button:has-text("Block")', 'button:has-text("No Thanks")']:
        try:
            btn = page.locator(selector)
            if btn.is_visible(timeout=2000):
                btn.click()
                break
        except Exception:
            pass

    # Navigate to AddOrder
    page.goto(ADD_ORDER_URL, wait_until="networkidle")
    print("AddOrder URL:", page.url)
    page.wait_for_timeout(2000)

    # Screenshot full page
    page.screenshot(path="/tmp/proteo_addorder.png", full_page=True)
    print("Screenshot saved to /tmp/proteo_addorder.png")

    # Dump all form fields
    fields = page.evaluate("""() => {
        const els = [...document.querySelectorAll('input, select, textarea')];
        return els.map(el => {
            const labelEl = document.querySelector('label[for="' + el.id + '"]');
            const tdLabel = el.closest('td')?.previousElementSibling?.textContent?.trim() || '';
            return {
                tag:         el.tagName,
                type:        el.type || '',
                name:        el.name || '',
                id:          el.id || '',
                value:       el.value || '',
                placeholder: el.placeholder || '',
                label:       labelEl?.textContent?.trim() || tdLabel || '',
                options:     el.tagName === 'SELECT'
                               ? [...el.options].map(o => ({v: o.value, t: o.text.trim()}))
                               : [],
            };
        });
    }""")

    print("\n=== FORM FIELDS ===")
    for f in fields:
        if f['type'] in ('hidden', 'submit', 'button') and not f['id']:
            continue
        print(json.dumps(f, ensure_ascii=False))

    # Also dump visible labels/td text to understand layout
    labels = page.evaluate("""() => {
        return [...document.querySelectorAll('td.fieldLabel, label, th')]
               .map(el => el.textContent?.trim())
               .filter(t => t && t.length < 60);
    }""")
    print("\n=== LABELS / TH ===")
    for l in labels:
        print(" ", l)

    # Dump the HTML around the collection point widget specifically
    print("\n=== COLLECTION POINT WIDGET HTML ===")
    col_html = page.evaluate("""() => {
        const el = document.getElementById('ctl00_ContentPlaceHolder1_ucOrder_ucCollectionPoint_cboPoint_Input');
        if (!el) return 'NOT FOUND';
        // Walk up to find the containing table/div
        let parent = el;
        for (let i = 0; i < 6; i++) parent = parent.parentElement;
        return parent ? parent.outerHTML.substring(0, 3000) : 'no parent';
    }""")
    print(col_html)

    browser.close()
    print("\nDone.")
