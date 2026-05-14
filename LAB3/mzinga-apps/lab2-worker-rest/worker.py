"""
Lab 2 Part A — REST API External Email Worker (State 2)

Architecture pattern: Strangler Fig + REST API Consumer + Polling

This worker:
1. Authenticates against the MZinga REST API using a JWT token
2. Polls GET /api/communications?where[status][equals]=pending&depth=1
3. Claims each document by PATCHing status="processing"
4. Extracts resolved email addresses from the depth=1 response (no MongoDB needed)
5. Serialises the Slate AST body to HTML
6. Sends the email via SMTP
7. Writes back status="sent" or status="failed" via PATCH

Run:
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python worker.py
"""

import os
import time
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import requests

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MZINGA_URL = os.environ["MZINGA_URL"]
MZINGA_EMAIL = os.environ["MZINGA_EMAIL"]
MZINGA_PASSWORD = os.environ["MZINGA_PASSWORD"]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def login() -> str:
    resp = requests.post(
        f"{MZINGA_URL}/api/users/login",
        json={"email": MZINGA_EMAIL, "password": MZINGA_PASSWORD},
    )
    resp.raise_for_status()
    log.info("Authenticated with MZinga API")
    return resp.json()["token"]


def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def api_request(token: str, method: str, path: str, **kwargs):
    url = f"{MZINGA_URL}{path}"
    resp = requests.request(method, url, headers=auth_headers(token), **kwargs)
    if resp.status_code == 401:
        log.warning("Token expired, re-authenticating")
        token = login()
        resp = requests.request(method, url, headers=auth_headers(token), **kwargs)
    resp.raise_for_status()
    return resp, token


# ---------------------------------------------------------------------------
# REST API helpers
# ---------------------------------------------------------------------------

def fetch_pending(token: str) -> tuple[list, str]:
    resp, token = api_request(
        token,
        "GET",
        "/api/communications",
        params={"where[status][equals]": "pending", "depth": 1},
    )
    return resp.json().get("docs", []), token


def update_status(token: str, doc_id: str, status: str):
    _, token = api_request(
        token,
        "PATCH",
        f"/api/communications/{doc_id}",
        json={"status": status},
    )
    return token


# ---------------------------------------------------------------------------
# Slate AST → HTML serialiser
# ---------------------------------------------------------------------------

def slate_to_html(nodes: list) -> str:
    html = ""
    for node in nodes or []:
        if node.get("type") == "paragraph":
            html += f"<p>{slate_to_html(node.get('children', []))}</p>"
        elif node.get("type") == "h1":
            html += f"<h1>{slate_to_html(node.get('children', []))}</h1>"
        elif node.get("type") == "h2":
            html += f"<h2>{slate_to_html(node.get('children', []))}</h2>"
        elif node.get("type") == "h3":
            html += f"<h3>{slate_to_html(node.get('children', []))}</h3>"
        elif node.get("type") == "h4":
            html += f"<h4>{slate_to_html(node.get('children', []))}</h4>"
        elif node.get("type") == "h5":
            html += f"<h5>{slate_to_html(node.get('children', []))}</h5>"
        elif node.get("type") == "h6":
            html += f"<h6>{slate_to_html(node.get('children', []))}</h6>"
        elif node.get("type") == "ul":
            html += f"<ul>{slate_to_html(node.get('children', []))}</ul>"
        elif node.get("type") == "ol":
            html += f"<ol>{slate_to_html(node.get('children', []))}</ol>"
        elif node.get("type") == "li":
            html += f"<li>{slate_to_html(node.get('children', []))}</li>"
        elif node.get("type") == "link":
            url = node.get("url", "#")
            html += f'<a href="{url}">{slate_to_html(node.get("children", []))}</a>'
        elif node.get("type") == "blockquote":
            html += f"<blockquote>{slate_to_html(node.get('children', []))}</blockquote>"
        elif node.get("type") == "upload":
            value = node.get("value", {})
            url = value.get("url", "")
            filename = value.get("filename", "attachment")
            if url:
                html += f'<p><a href="{url}">{filename}</a></p>'
        elif "text" in node:
            text = node["text"]
            if node.get("bold"):
                text = f"<strong>{text}</strong>"
            if node.get("italic"):
                text = f"<em>{text}</em>"
            if node.get("underline"):
                text = f"<u>{text}</u>"
            if node.get("strikethrough"):
                text = f"<s>{text}</s>"
            if node.get("code"):
                text = f"<code>{text}</code>"
            html += text
        else:
            html += slate_to_html(node.get("children", []))
    return html


# ---------------------------------------------------------------------------
# Recipient extraction (depth=1 returns full value objects)
# ---------------------------------------------------------------------------

def extract_emails(relationship_list: list) -> list[str]:
    emails = []
    for r in relationship_list or []:
        value = r.get("value") or {}
        if isinstance(value, dict) and value.get("email"):
            emails.append(value["email"])
    return emails


# ---------------------------------------------------------------------------
# SMTP delivery
# ---------------------------------------------------------------------------

def send_email(to_addresses: list[str], subject: str, html: str,
               cc_addresses: list[str] = None, bcc_addresses: list[str] = None):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)
    msg.attach(MIMEText(html, "html"))
    all_recipients = to_addresses + (cc_addresses or []) + (bcc_addresses or [])
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.sendmail(EMAIL_FROM, all_recipients, msg.as_string())


# ---------------------------------------------------------------------------
# Document processor
# ---------------------------------------------------------------------------

def process(token: str, doc: dict):
    doc_id = doc["id"]
    log.info(f"Processing communication {doc_id}")
    token = update_status(token, doc_id, "processing")
    try:
        to_emails = extract_emails(doc.get("tos"))
        if not to_emails:
            raise ValueError("No valid 'to' email addresses found")
        cc_emails = extract_emails(doc.get("ccs"))
        bcc_emails = extract_emails(doc.get("bccs"))
        html = slate_to_html(doc.get("body") or [])
        send_email(to_emails, doc["subject"], html, cc_emails, bcc_emails)
        token = update_status(token, doc_id, "sent")
        log.info(f"Communication {doc_id} sent successfully")
    except Exception as e:
        log.error(f"Failed to process communication {doc_id}: {e}")
        token = update_status(token, doc_id, "failed")
    return token


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def poll():
    token = login()
    log.info(f"Worker started. Polling every {POLL_INTERVAL}s")
    while True:
        try:
            docs, token = fetch_pending(token)
            for doc in docs:
                token = process(token, doc)
            if not docs:
                time.sleep(POLL_INTERVAL)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.error(
                    "MZinga returned 404 for the communications API. "
                    "Check that the logged-in user has the admin role and that "
                    "the Communications collection is enabled."
                )
                log.error(f"Response body: {e.response.text}")
            else:
                log.error(f"HTTP error: {e}")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Worker stopped.")
            break


if __name__ == "__main__":
    poll()
