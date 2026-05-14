"""
Lab 2 Part B — Event-Driven External Email Worker (State 3)

Architecture pattern: Strangler Fig + REST API Consumer + RabbitMQ Subscriber

This worker:
1. Authenticates against the MZinga REST API using a JWT token
2. Connects to RabbitMQ and subscribes to the mzinga_events_durable exchange
3. Receives afterChange events for the Communications collection (no polling)
4. Filters out "update" operations to avoid infinite loops from its own PATCH write-backs
5. Fetches the full document via GET /api/communications/:id?depth=1
6. Applies an idempotency guard: skips documents already "sent" or "processing"
7. Extracts resolved email addresses, serialises the body, sends the email
8. Acknowledges the RabbitMQ message only after successful processing

Run:
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python worker.py
"""

import asyncio
import os
import json
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import aio_pika
import requests

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
ROUTING_KEY = os.environ["ROUTING_KEY"]
EXCHANGE_NAME = os.environ["EXCHANGE_NAME"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
MZINGA_URL = os.environ["MZINGA_URL"]
MZINGA_EMAIL = os.environ["MZINGA_EMAIL"]
MZINGA_PASSWORD = os.environ["MZINGA_PASSWORD"]
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


# ---------------------------------------------------------------------------
# REST API helpers
# ---------------------------------------------------------------------------

def fetch_doc(token: str, doc_id: str) -> dict:
    resp = requests.get(
        f"{MZINGA_URL}/api/communications/{doc_id}",
        params={"depth": 1},
        headers=auth_headers(token),
    )
    resp.raise_for_status()
    return resp.json()


def update_status(token: str, doc_id: str, status: str):
    resp = requests.patch(
        f"{MZINGA_URL}/api/communications/{doc_id}",
        json={"status": status},
        headers=auth_headers(token),
    )
    resp.raise_for_status()


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

def process(token: str, doc: dict) -> str:
    doc_id = doc["id"]

    # Idempotency guard: skip if already processed or in-flight
    if doc.get("status") in ("sent", "processing"):
        log.info(f"Skipping {doc_id} — already {doc['status']}")
        return token

    log.info(f"Processing communication {doc_id}")
    update_status(token, doc_id, "processing")

    try:
        to_emails = extract_emails(doc.get("tos"))
        if not to_emails:
            raise ValueError("No valid 'to' email addresses found")
        cc_emails = extract_emails(doc.get("ccs"))
        bcc_emails = extract_emails(doc.get("bccs"))
        html = slate_to_html(doc.get("body") or [])
        send_email(to_emails, doc["subject"], html, cc_emails, bcc_emails)
        update_status(token, doc_id, "sent")
        log.info(f"Communication {doc_id} sent successfully")
    except Exception as e:
        log.error(f"Failed to process communication {doc_id}: {e}")
        update_status(token, doc_id, "failed")

    return token


# ---------------------------------------------------------------------------
# Main event-driven loop
# ---------------------------------------------------------------------------

async def main():
    token = login()

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        # Deliver one message at a time — safe for multiple concurrent workers
        await channel.set_qos(prefetch_count=1)

        # Declare the exchange with the same params MZinga uses
        exchange = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC,
            durable=True, internal=True, auto_delete=False,
        )

        # Named durable queue — survives worker restarts; messages queue up while down
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        log.info(f"Subscribed to {EXCHANGE_NAME} with routing key {ROUTING_KEY}. Waiting for messages.")

        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process(requeue=True):
                    try:
                        body = json.loads(message.body.decode())
                        event_data = body.get("data", {})
                        operation = event_data.get("operation")
                        doc_id = (event_data.get("doc") or {}).get("id")

                        if not doc_id:
                            log.warning("Message missing doc.id, skipping")
                            continue

                        # Filter out "update" operations to avoid an infinite loop:
                        # the worker's own PATCH status write-back triggers another
                        # afterChange event with operation="update"
                        if operation != "create":
                            log.debug(f"Ignoring operation={operation} for {doc_id}")
                            continue

                        doc = fetch_doc(token, doc_id)
                        token = process(token, doc)

                    except requests.HTTPError as e:
                        if e.response.status_code == 401:
                            log.warning("Token expired, re-authenticating")
                            token = login()
                        else:
                            log.error(f"HTTP error processing message: {e}")
                            raise
                    except KeyboardInterrupt:
                        log.info("Worker stopped.")
                        return


if __name__ == "__main__":
    asyncio.run(main())
