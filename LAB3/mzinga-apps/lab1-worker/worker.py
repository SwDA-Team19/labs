"""
Lab 1 — DB-Coupled External Email Worker (State 1)

Architecture pattern: Strangler Fig + Shared Database + Polling Consumer

This worker:
1. Polls the MongoDB `communications` collection for documents with status="pending"
2. Claims each document immediately by setting status="processing"
3. Resolves recipient email addresses from the `users` collection
4. Serialises the Slate AST body field to HTML
5. Sends the email via SMTP (MailHog locally, or any real SMTP server)
6. Writes back status="sent" on success, or status="failed" on error

Run:
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python worker.py
"""

import os
import time
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin@localhost:27017/mzinga?authSource=admin&directConnection=true")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Slate AST → HTML serialiser
# ---------------------------------------------------------------------------

def serialize_node(node: dict) -> str:
    """
    Recursively converts a single Slate AST node to an HTML string.
    Handles the node types used by MZinga's rich-text editor.
    """
    # Leaf text node — apply inline marks
    if "text" in node:
        text = node["text"]
        if not text:
            return ""
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
        return text

    # Recurse into children first
    children_html = "".join(serialize_node(child) for child in node.get("children", []))

    node_type = node.get("type", "")

    if node_type == "paragraph":
        return f"<p>{children_html}</p>"
    elif node_type == "h1":
        return f"<h1>{children_html}</h1>"
    elif node_type == "h2":
        return f"<h2>{children_html}</h2>"
    elif node_type == "h3":
        return f"<h3>{children_html}</h3>"
    elif node_type == "h4":
        return f"<h4>{children_html}</h4>"
    elif node_type == "h5":
        return f"<h5>{children_html}</h5>"
    elif node_type == "h6":
        return f"<h6>{children_html}</h6>"
    elif node_type == "ul":
        return f"<ul>{children_html}</ul>"
    elif node_type == "ol":
        return f"<ol>{children_html}</ol>"
    elif node_type == "li":
        return f"<li>{children_html}</li>"
    elif node_type == "link":
        url = node.get("url", "#")
        return f'<a href="{url}">{children_html}</a>'
    elif node_type == "blockquote":
        return f"<blockquote>{children_html}</blockquote>"
    elif node_type == "upload":
        # Render as a download link if a URL is available
        value = node.get("value", {})
        url = value.get("url", "")
        filename = value.get("filename", "attachment")
        if url:
            return f'<p><a href="{url}">{filename}</a></p>'
        return ""
    else:
        # Unknown block type — fall back to a plain paragraph
        return f"<p>{children_html}</p>" if children_html else ""


def slate_to_html(body) -> str:
    """
    Convert the entire Slate AST body (a list of top-level nodes) to HTML.
    The body field may be None, an empty string, or a list of node dicts.
    """
    if not body or not isinstance(body, list):
        return ""
    return "".join(serialize_node(node) for node in body)


# ---------------------------------------------------------------------------
# Recipient resolution
# ---------------------------------------------------------------------------

def resolve_emails(db, relationship_list: list) -> list[str]:
    """
    Resolve a Payload CMS relationship list to a flat list of email strings.

    Each entry looks like:
        { "relationTo": "users", "value": ObjectId("...") }

    The ObjectId may already be an ObjectId instance (pymongo) or a string.
    """
    if not relationship_list:
        return []

    # Group by collection in case future versions support cross-collection tos
    by_collection: dict[str, list] = {}
    for ref in relationship_list:
        coll = ref.get("relationTo", "users")
        raw_id = ref.get("value")
        # Normalise: the value is stored as an ObjectId in MongoDB
        if isinstance(raw_id, dict):
            # Occasionally the full sub-document is embedded
            raw_id = raw_id.get("id") or raw_id.get("_id")
        try:
            oid = ObjectId(raw_id)
        except Exception:
            log.warning("Could not convert %s to ObjectId, skipping", raw_id)
            continue
        by_collection.setdefault(coll, []).append(oid)

    emails: list[str] = []
    for coll, ids in by_collection.items():
        cursor = db[coll].find({"_id": {"$in": ids}}, {"email": 1})
        for user in cursor:
            if user.get("email"):
                emails.append(user["email"])
    return emails


# ---------------------------------------------------------------------------
# SMTP delivery
# ---------------------------------------------------------------------------

def send_email(to_list: list[str], cc_str: str, bcc_str: str, subject: str, html: str) -> None:
    """
    Send a single HTML email to each address in to_list via SMTP.
    Uses Python's built-in smtplib — no external library required.
    MailHog (docker run -d -p 1025:1025 -p 8025:8025 mailhog/mailhog)
    works out of the box on SMTP_PORT=1025 without authentication.
    """
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        for to_addr in to_list:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = EMAIL_FROM
            msg["To"] = to_addr
            if cc_str:
                msg["Cc"] = cc_str
            # BCC headers are intentionally omitted from the message headers
            # but included in the SMTP envelope recipients
            envelope_recipients = [to_addr]
            if cc_str:
                envelope_recipients += [a.strip() for a in cc_str.split(",") if a.strip()]
            if bcc_str:
                envelope_recipients += [a.strip() for a in bcc_str.split(",") if a.strip()]

            msg.attach(MIMEText(html, "html"))
            server.sendmail(EMAIL_FROM, envelope_recipients, msg.as_string())
            log.info("  → sent to %s", to_addr)


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def process_document(db, doc: dict) -> None:
    """
    Process a single Communications document end-to-end:
    claim → resolve → serialise → send → write result
    """
    doc_id = doc["_id"]
    subject = doc.get("subject", "(no subject)")
    log.info("Processing document %s  subject=%r", doc_id, subject)

    # Step 3 — Claim: mark as processing before any work to prevent double-processing
    db.communications.update_one(
        {"_id": doc_id},
        {"$set": {"status": "processing"}},
    )

    try:
        # Step 4 — Resolve recipients
        tos = doc.get("tos") or []
        ccs = doc.get("ccs") or []
        bccs = doc.get("bccs") or []

        to_emails = resolve_emails(db, tos)
        cc_emails = resolve_emails(db, ccs)
        bcc_emails = resolve_emails(db, bccs)

        if not to_emails:
            raise ValueError("No valid 'to' email addresses could be resolved")

        cc_str = ", ".join(cc_emails)
        bcc_str = ", ".join(bcc_emails)

        # Step 5 — Serialise Slate AST body → HTML
        html = slate_to_html(doc.get("body"))

        # Step 6 — Send
        send_email(to_emails, cc_str, bcc_str, subject, html)

        # Step 7 — Mark sent
        db.communications.update_one(
            {"_id": doc_id},
            {"$set": {"status": "sent"}},
        )
        log.info("Document %s → sent", doc_id)

    except Exception as exc:
        # Step 7 (error) — Mark failed and log
        log.error("Document %s failed: %s", doc_id, exc)
        db.communications.update_one(
            {"_id": doc_id},
            {"$set": {"status": "failed"}},
        )


def run() -> None:
    log.info("Lab 1 worker starting (SMTP=%s:%s  poll=%ss)", SMTP_HOST, SMTP_PORT, POLL_INTERVAL)

    # Step 1 — Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client.get_default_database()
    log.info("Connected to MongoDB database %r", db.name)

    while True:
        try:
            # Step 2 — Poll for one pending document
            doc = db.communications.find_one({"status": "pending"})
            if doc is None:
                log.debug("No pending documents. Sleeping %ss…", POLL_INTERVAL)
                time.sleep(POLL_INTERVAL)
                continue

            process_document(db, doc)

        except PyMongoError as exc:
            log.error("MongoDB error: %s — retrying in %ss", exc, POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Worker stopped.")
            break


if __name__ == "__main__":
    run()
