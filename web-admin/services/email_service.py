from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage


def _enabled() -> bool:
    return os.getenv("EMAIL_NOTIFICATIONS_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}


def _split_recipients(raw: str) -> list[str]:
    parts = raw.replace(";", ",").split(",")
    return [p.strip() for p in parts if p.strip()]


def _recipients_for_category(category: str) -> list[str]:
    cat = category.strip().lower()
    if cat == "docflow":
        return _split_recipients(os.getenv("EMAIL_DOCFLOW_TO", ""))
    if cat == "order":
        return _split_recipients(os.getenv("EMAIL_ORDER_TO", ""))
    if cat == "order_print":
        return _split_recipients(os.getenv("EMAIL_CASHIER_TO", ""))
    if cat == "docflow_lawyer":
        return _split_recipients(os.getenv("EMAIL_DOCFLOW_LAWYER_TO", ""))
    if cat == "docflow_agent_task":
        return _split_recipients(os.getenv("EMAIL_DOCFLOW_AGENT_TO", ""))
    return _split_recipients(os.getenv("EMAIL_DEFAULT_TO", ""))


def send_category_notification_email(category: str, title: str, message: str, link: str = "") -> tuple[bool, str]:
    if not _enabled():
        return False, "EMAIL_NOTIFICATIONS_ENABLED=0"

    recipients = _recipients_for_category(category)
    if not recipients:
        return False, f"Нет получателей для категории {category}"

    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "587").strip() or "587")
    username = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    from_email = os.getenv("SMTP_FROM", username).strip()
    use_ssl = os.getenv("SMTP_USE_SSL", "0").strip().lower() in {"1", "true", "yes", "on"}
    use_tls = os.getenv("SMTP_USE_TLS", "1").strip().lower() in {"1", "true", "yes", "on"}
    base_url = os.getenv("APP_PUBLIC_URL", "https://xn--h1aaaawb0bm.online").strip().rstrip("/")

    if not host or not from_email:
        return False, "Не заполнены SMTP_HOST/SMTP_FROM"

    app_link = f"{base_url}{link}" if link.startswith("/") else (link or base_url)
    body = f"{message}\n\nСсылка: {app_link}"

    msg = EmailMessage()
    msg["Subject"] = f"[WebAdmin] {title}"
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        if use_ssl:
            with smtplib.SMTP_SSL(host, port, timeout=15) as smtp:
                if username:
                    smtp.login(username, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=15) as smtp:
                if use_tls:
                    smtp.starttls()
                if username:
                    smtp.login(username, password)
                smtp.send_message(msg)
    except Exception as exc:
        return False, str(exc)

    return True, "OK"
