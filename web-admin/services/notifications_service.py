from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from services.email_service import send_category_notification_email


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "storage" / "notifications.db"


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT NOT NULL DEFAULT '',
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def add_notification(category: str, title: str, message: str, link: str = "") -> None:
    _ensure_db()
    category_clean = category.strip()
    title_clean = title.strip()
    message_clean = message.strip()
    link_clean = link.strip()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO notifications (category, title, message, link, is_read) VALUES (?, ?, ?, ?, 0)",
            (category_clean, title_clean, message_clean, link_clean),
        )
        conn.commit()
    send_category_notification_email(category_clean, title_clean, message_clean, link_clean)


def list_notifications(limit: int = 200) -> list[dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, category, title, message, link, is_read, created_at
            FROM notifications
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(int(limit), 1),),
        ).fetchall()
    return [dict(r) for r in rows]


def unread_count() -> int:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COUNT(*) FROM notifications WHERE is_read = 0").fetchone()
    return int(row[0] if row else 0)


def mark_read(notification_id: int) -> bool:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("UPDATE notifications SET is_read = 1 WHERE id = ?", (notification_id,))
        conn.commit()
    return cur.rowcount > 0


def mark_all_read() -> int:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("UPDATE notifications SET is_read = 1 WHERE is_read = 0")
        conn.commit()
    return int(cur.rowcount or 0)
