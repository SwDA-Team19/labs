"""
Lab 3 — Observable REST API Email Worker

This worker polls MZinga for pending Communications documents, sends email
through SMTP, and exposes structured logs, OpenTelemetry traces, and Prometheus
metrics.
"""

from __future__ import annotations

import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
import structlog
from dotenv import load_dotenv
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode
from prometheus_client import start_http_server


load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

MZINGA_URL = os.environ["MZINGA_URL"]
MZINGA_EMAIL = os.environ["MZINGA_EMAIL"]
MZINGA_PASSWORD = os.environ["MZINGA_PASSWORD"]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"
)
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "email-worker")
OTEL_SERVICE_VERSION = os.getenv("OTEL_SERVICE_VERSION", "3.0.0")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))


def add_service_name(_, __, event_dict: dict) -> dict:
    event_dict["service"] = OTEL_SERVICE_NAME
    return event_dict


def add_trace_context(_, __, event_dict: dict) -> dict:
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if span_context.is_valid:
        event_dict["trace_id"] = format(span_context.trace_id, "032x")
        event_dict["span_id"] = format(span_context.span_id, "016x")
    return event_dict


def configure_logging() -> structlog.stdlib.BoundLogger:
    logging.basicConfig(level=logging.INFO, format="%(message)s\n")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            add_service_name,
            add_trace_context,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger(__name__)


def configure_observability() -> tuple[trace.Tracer, metrics.Meter]:
    resource = Resource.create(
        {
            "service.name": OTEL_SERVICE_NAME,
            "service.version": OTEL_SERVICE_VERSION,
        }
    )

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{OTEL_EXPORTER_OTLP_ENDPOINT}/v1/traces")
        )
    )
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PrometheusMetricReader()
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)
    start_http_server(PROMETHEUS_PORT)

    RequestsInstrumentor().instrument()

    return trace.get_tracer(OTEL_SERVICE_NAME), metrics.get_meter(OTEL_SERVICE_NAME)


log = configure_logging()
tracer, meter = configure_observability()

emails_processed_total = meter.create_counter(
    "emails_processed_total",
    description="Number of processed emails partitioned by outcome.",
)
email_processing_duration_seconds = meter.create_histogram(
    "email_processing_duration_seconds",
    unit="s",
    description="End-to-end communication processing duration.",
)
smtp_send_duration_seconds = meter.create_histogram(
    "smtp_send_duration_seconds",
    unit="s",
    description="SMTP send duration.",
)
worker_poll_total = meter.create_counter(
    "worker_poll_total",
    description="Number of worker poll cycles partitioned by result.",
)


def login() -> str:
    resp = requests.post(
        f"{MZINGA_URL}/api/users/login",
        json={"email": MZINGA_EMAIL, "password": MZINGA_PASSWORD},
    )
    resp.raise_for_status()
    log.info("authenticated")
    return resp.json()["token"]


def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def api_request(token: str, method: str, path: str, **kwargs):
    url = f"{MZINGA_URL}{path}"
    resp = requests.request(method, url, headers=auth_headers(token), **kwargs)
    if resp.status_code == 401:
        log.warning("token_expired")
        token = login()
        resp = requests.request(method, url, headers=auth_headers(token), **kwargs)
    resp.raise_for_status()
    return resp, token


def fetch_pending(token: str) -> tuple[list, str]:
    resp, token = api_request(
        token,
        "GET",
        "/api/communications",
        params={"where[status][equals]": "pending", "depth": 1},
    )
    return resp.json().get("docs", []), token


def update_status(token: str, doc_id: str, status: str) -> str:
    _, token = api_request(
        token,
        "PATCH",
        f"/api/communications/{doc_id}",
        json={"status": status},
    )
    return token


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


def extract_emails(relationship_list: list) -> list[str]:
    emails = []
    for relationship in relationship_list or []:
        value = relationship.get("value") or {}
        if isinstance(value, dict) and value.get("email"):
            emails.append(value["email"])
    return emails


def send_email(
    to_addresses: list[str],
    subject: str,
    html: str,
    cc_addresses: list[str] | None = None,
    bcc_addresses: list[str] | None = None,
):
    all_recipients = to_addresses + (cc_addresses or []) + (bcc_addresses or [])
    send_started = time.perf_counter()
    with tracer.start_as_current_span("send_email") as span:
        span.set_attribute("recipient_count", len(to_addresses))
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(to_addresses)
        if cc_addresses:
            msg["Cc"] = ", ".join(cc_addresses)
        msg.attach(MIMEText(html, "html"))

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.sendmail(EMAIL_FROM, all_recipients, msg.as_string())
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            smtp_send_duration_seconds.record(time.perf_counter() - send_started)


def process(token: str, doc: dict) -> str:
    doc_id = doc["id"]
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(doc_id=doc_id)
    processing_started = time.perf_counter()
    status = "failed"
    recipient_count = 0

    with tracer.start_as_current_span("process_communication") as span:
        span.set_attribute("doc_id", doc_id)
        log.info("processing_communication")
        token = update_status(token, doc_id, "processing")

        try:
            to_emails = extract_emails(doc.get("tos"))
            if not to_emails:
                raise ValueError("No valid 'to' email addresses found")

            cc_emails = extract_emails(doc.get("ccs"))
            bcc_emails = extract_emails(doc.get("bccs"))
            recipient_count = len(to_emails)

            with tracer.start_as_current_span("serialize_body") as serialize_span:
                body_nodes = doc.get("body") or []
                serialize_span.set_attribute("node_count", len(body_nodes))
                html = slate_to_html(body_nodes)

            send_email(to_emails, doc["subject"], html, cc_emails, bcc_emails)
            token = update_status(token, doc_id, "sent")
            status = "sent"
            log.info("communication_sent", recipient_count=recipient_count)
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            log.exception("communication_failed", error=str(exc))
            token = update_status(token, doc_id, "failed")
        finally:
            elapsed = time.perf_counter() - processing_started
            emails_processed_total.add(
                1,
                {"status": status, "recipient_count": recipient_count},
            )
            email_processing_duration_seconds.record(elapsed)
            structlog.contextvars.clear_contextvars()

    return token


def poll():
    token = login()
    log.info(
        "worker_started",
        poll_interval_seconds=POLL_INTERVAL,
        prometheus_port=PROMETHEUS_PORT,
    )
    while True:
        try:
            docs, token = fetch_pending(token)
            poll_result = "found" if docs else "empty"
            worker_poll_total.add(1, {"result": poll_result})

            for doc in docs:
                token = process(token, doc)

            if not docs:
                time.sleep(POLL_INTERVAL)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                log.error(
                    "communications_api_not_found",
                    response_body=exc.response.text,
                )
            else:
                log.exception("http_error", error=str(exc))
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("worker_stopped")
            break


if __name__ == "__main__":
    poll()
