from __future__ import annotations

import sqlite3
import hashlib
import shutil
import json
import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from urllib import request as urlrequest
from urllib import error as urlerror
from urllib.parse import urlencode, quote

from openpyxl import load_workbook


BASE = Path("/root/webadminbots/Infinity Projects")
APP_ROOT = Path(__file__).resolve().parent.parent
ORDER_DOC_TEMPLATES_DIR = APP_ROOT / "templates" / "order_docs"


def _ensure_db_file(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        with sqlite3.connect(db_path):
            pass


def db_paths() -> dict[str, Path]:
    return {
        "meeting": BASE / "Meeting-booking-bot" / "meeting_bot.db",
        "broker": BASE / "Broker-booking-bot" / "broker_booking.db",
        "order": BASE / "order-bot" / "storage" / "orders.db",
        "contracts": BASE / "contract-register" / "contracts.db",
        "docflow": BASE / "doc-flow-bot" / "app" / "database.db",
    }


def _query(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql, params).fetchall()


def _execute(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount


def _table_columns(db_path: Path, table: str) -> set[str]:
    rows = _query(db_path, f"PRAGMA table_info({table})")
    return {str(row["name"]) for row in rows}


def _password_hash(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _ensure_web_users_table(db_path: Path) -> None:
    _ensure_db_file(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
        """
        CREATE TABLE IF NOT EXISTS web_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
        )
        conn.commit()


def web_register_user(module: str, telegram_id: str, email: str, password: str) -> tuple[bool, str]:
    path = db_paths().get(module)
    if path is None:
        return False, "Неизвестный модуль"
    _ensure_web_users_table(path)
    clean_email = email.strip().lower()
    if "@" not in clean_email or len(password.strip()) < 4:
        return False, "Некорректная почта или слишком короткий пароль"
    exists = _query(path, "SELECT id FROM web_users WHERE email = ? LIMIT 1", (clean_email,))
    if exists:
        return False, "Почта уже зарегистрирована"
    _execute(
        path,
        "INSERT INTO web_users (telegram_id, email, password_hash) VALUES (?, ?, ?)",
        (str(telegram_id).strip(), clean_email, _password_hash(password)),
    )
    return True, "Веб-аккаунт создан"


def web_authenticate(module: str, email: str, password: str) -> tuple[bool, str, str | None]:
    path = db_paths().get(module)
    if path is None:
        return False, "Неизвестный модуль", None
    _ensure_web_users_table(path)
    clean_email = email.strip().lower()
    rows = _query(
        path,
        "SELECT telegram_id, password_hash FROM web_users WHERE email = ? LIMIT 1",
        (clean_email,),
    )
    if not rows:
        return False, "Пользователь с такой почтой не найден", None
    row = rows[0]
    if row["password_hash"] != _password_hash(password):
        return False, "Неверный пароль", None
    return True, "Вход выполнен", str(row["telegram_id"])


def _module_user_table(module: str) -> tuple[Path, str, str] | None:
    path = db_paths().get(module)
    if path is None:
        return None
    if module in {"meeting", "broker", "order", "contracts"}:
        return path, "users", "telegram_id"
    if module == "docflow":
        return path, "users", "telegram_id"
    return None


def _telegram_exists(module: str, telegram_id: str) -> bool:
    info = _module_user_table(module)
    if info is None:
        return False
    path, table, field = info
    rows = _query(path, f"SELECT {field} FROM {table} WHERE {field} = ? LIMIT 1", (telegram_id,))
    return bool(rows)


def generate_telegram_id(module: str, email: str) -> str:
    seed = int(hashlib.sha256(f"{module}:{email.strip().lower()}".encode("utf-8")).hexdigest()[:12], 16)
    candidate = 1_000_000_000 + (seed % 8_000_000_000)
    for _ in range(1000):
        text = str(candidate)
        if not _telegram_exists(module, text):
            return text
        candidate += 97
        if candidate > 9_999_999_999:
            candidate = 1_000_000_000 + (candidate % 9_000_000_000)
    raise RuntimeError("Не удалось сгенерировать уникальный telegram_id")


# -------------------------
# Meeting booking module
# -------------------------

def meeting_register_user(telegram_id: int, full_name: str, department: str) -> tuple[bool, str]:
    role = "user"
    _execute(
        db_paths()["meeting"],
        "INSERT OR REPLACE INTO users (telegram_id, full_name, department, role) VALUES (?, ?, ?, ?)",
        (telegram_id, full_name.strip(), department.strip(), role),
    )
    return True, "Пользователь зарегистрирован"


def meeting_register_web(full_name: str, department: str, email: str, account_password: str) -> tuple[bool, str]:
    telegram_id = int(generate_telegram_id("meeting", email))
    ok, text = meeting_register_user(telegram_id, full_name, department)
    if not ok:
        return ok, text
    ok2, text2 = web_register_user("meeting", str(telegram_id), email, account_password)
    if not ok2:
        return False, text2
    return True, "Пользователь зарегистрирован"


def meeting_users() -> list[dict[str, Any]]:
    rows = _query(
        db_paths()["meeting"],
        "SELECT telegram_id, full_name, department, role FROM users ORDER BY telegram_id DESC LIMIT 300",
    )
    return [dict(r) for r in rows]


def meeting_users_with_email() -> list[dict[str, Any]]:
    rows = _query(
        db_paths()["meeting"],
        """
        SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department, u.role
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
        ORDER BY u.telegram_id DESC
        LIMIT 300
        """,
    )
    return [dict(r) for r in rows]


def meeting_get_user(telegram_id: int) -> dict[str, Any] | None:
    rows = _query(
        db_paths()["meeting"],
        "SELECT telegram_id, full_name, department, role FROM users WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    )
    return dict(rows[0]) if rows else None


def meeting_rooms() -> list[dict[str, Any]]:
    db = db_paths()["meeting"]
    rows = _query(
        db,
        "SELECT id, name, capacity, is_active, room_type FROM rooms ORDER BY id ASC",
    )
    if not rows:
        defaults = [
            ("Сириус", 6, 1, "meeting"),
            ("Солнце", 6, 1, "meeting"),
            ("Арктур", 4, 1, "meeting"),
            ("Бетельгейзе", 3, 1, "meeting"),
            ("Вега", 3, 1, "meeting"),
            ("Мансарда", 100, 1, "class"),
            ("Аквариум", 20, 1, "class"),
            ("Малый класс", 20, 1, "class"),
        ]
        for name, capacity, is_active, room_type in defaults:
            _execute(
                db,
                "INSERT INTO rooms (name, capacity, is_active, room_type) VALUES (?, ?, ?, ?)",
                (name, capacity, is_active, room_type),
            )
        rows = _query(
            db,
            "SELECT id, name, capacity, is_active, room_type FROM rooms ORDER BY id ASC",
        )
    return [dict(r) for r in rows]


def meeting_create_booking(user_id: int, room_id: int, start_time: str, end_time: str, title: str) -> tuple[bool, str]:
    db = db_paths()["meeting"]
    try:
        s = datetime.fromisoformat(start_time)
        e = datetime.fromisoformat(end_time)
    except ValueError:
        return False, "Некорректный формат даты/времени"
    if e <= s:
        return False, "Время окончания должно быть позже начала"

    room = _query(db, "SELECT id, COALESCE(is_active, 1) AS is_active FROM rooms WHERE id = ? LIMIT 1", (room_id,))
    if not room:
        return False, "Переговорная не найдена"
    if int(room[0]["is_active"] or 0) != 1:
        return False, "Переговорная недоступна для бронирования"

    conflicts = _query(
        db,
        """
        SELECT id
        FROM bookings
        WHERE room_id = ?
          AND start_time < ?
          AND end_time > ?
        LIMIT 1
        """,
        (room_id, e.isoformat(sep=" "), s.isoformat(sep=" ")),
    )
    if conflicts:
        return False, "Выбранный интервал пересекается с существующим бронированием"

    cols = _table_columns(db, "bookings")
    fields = ["user_id", "room_id", "start_time", "end_time", "title"]
    values: list[Any] = [user_id, room_id, s.isoformat(sep=" "), e.isoformat(sep=" "), title.strip()]
    if "reminder_sent" in cols:
        fields.append("reminder_sent")
        values.append(0)
    sql = f"INSERT INTO bookings ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
    _execute(db, sql, tuple(values))
    return True, "Бронирование создано"


def meeting_bookings(user_id: int | None = None, all_rows: bool = False) -> list[dict[str, Any]]:
    db = db_paths()["meeting"]
    sql = """
    SELECT b.id, b.user_id, u.full_name, r.name AS room_name, b.start_time, b.end_time, b.title
    FROM bookings b
    LEFT JOIN users u ON u.telegram_id = b.user_id
    LEFT JOIN rooms r ON r.id = b.room_id
    """
    params: tuple[Any, ...] = ()
    if not all_rows and user_id is not None:
        sql += " WHERE b.user_id = ? AND b.end_time > datetime('now')"
        params = (user_id,)
    elif not all_rows:
        sql += " WHERE b.end_time > datetime('now')"
    sql += " ORDER BY b.start_time ASC LIMIT 500"
    rows = _query(db, sql, params)
    return [dict(r) for r in rows]


def meeting_cancel_booking(booking_id: int, actor_user_id: int, actor_role: str) -> tuple[bool, str]:
    db = db_paths()["meeting"]
    row = _query(db, "SELECT user_id FROM bookings WHERE id = ? LIMIT 1", (booking_id,))
    if not row:
        return False, "Бронь не найдена"
    owner_id = int(row[0]["user_id"])
    if actor_user_id != owner_id and actor_role not in {"hr", "admin"}:
        return False, "Недостаточно прав для отмены"
    affected = _execute(db, "DELETE FROM bookings WHERE id = ?", (booking_id,))
    return (affected > 0, "Бронь отменена" if affected > 0 else "Бронь не найдена")


def meeting_update_role(telegram_id: int, role: str) -> tuple[bool, str]:
    affected = _execute(db_paths()["meeting"], "UPDATE users SET role = ? WHERE telegram_id = ?", (role, telegram_id))
    return (affected > 0, "Роль обновлена" if affected > 0 else "Пользователь не найден")


def meeting_update_role_by_email(email: str, role: str) -> tuple[bool, str]:
    clean_email = email.strip().lower()
    if clean_email == "":
        return False, "Укажите email"
    rows = _query(
        db_paths()["meeting"],
        "SELECT telegram_id FROM web_users WHERE lower(email) = lower(?) LIMIT 1",
        (clean_email,),
    )
    if not rows:
        return False, "Пользователь с таким email не найден"
    return meeting_update_role(int(rows[0]["telegram_id"]), role)


# -------------------------
# Broker booking module
# -------------------------

def _ensure_broker_schema() -> None:
    db = db_paths()["broker"]
    _ensure_db_file(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                department TEXT NOT NULL,
                role TEXT DEFAULT 'user'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                capacity INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1,
                room_type TEXT DEFAULT 'broker'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                room_id INTEGER NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                title TEXT NOT NULL,
                reminder_sent INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def broker_register_user(telegram_id: int, full_name: str, department: str) -> tuple[bool, str]:
    _ensure_broker_schema()
    role = "user"
    _execute(
        db_paths()["broker"],
        "INSERT OR REPLACE INTO users (telegram_id, full_name, department, role) VALUES (?, ?, ?, ?)",
        (telegram_id, full_name.strip(), department.strip(), role),
    )
    return True, "Пользователь зарегистрирован"


def broker_register_web(full_name: str, department: str, email: str, account_password: str) -> tuple[bool, str]:
    _ensure_broker_schema()
    telegram_id = int(generate_telegram_id("broker", email))
    ok, text = broker_register_user(telegram_id, full_name, department)
    if not ok:
        return ok, text
    ok2, text2 = web_register_user("broker", str(telegram_id), email, account_password)
    if not ok2:
        return False, text2
    return True, "Пользователь зарегистрирован"


def broker_users_with_email() -> list[dict[str, Any]]:
    _ensure_broker_schema()
    rows = _query(
        db_paths()["broker"],
        """
        SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department, u.role
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = CAST(u.telegram_id AS TEXT)
        ORDER BY u.telegram_id DESC
        LIMIT 300
        """,
    )
    return [dict(r) for r in rows]


def broker_get_user(telegram_id: int) -> dict[str, Any] | None:
    _ensure_broker_schema()
    rows = _query(
        db_paths()["broker"],
        "SELECT telegram_id, full_name, department, role FROM users WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    )
    return dict(rows[0]) if rows else None


def broker_rooms() -> list[dict[str, Any]]:
    _ensure_broker_schema()
    db = db_paths()["broker"]
    rows = _query(
        db,
        "SELECT id, name, capacity, is_active, room_type FROM rooms ORDER BY id ASC",
    )
    if not rows:
        defaults = [
            ("Брокер 1", 1, 1, "broker"),
            ("Брокер 2", 1, 1, "broker"),
            ("Брокер 3", 1, 1, "broker"),
            ("Брокер 4", 1, 1, "broker"),
            ("Брокер 5", 1, 1, "broker"),
            ("Брокер 6", 1, 1, "broker"),
        ]
        for name, capacity, is_active, room_type in defaults:
            _execute(
                db,
                "INSERT INTO rooms (name, capacity, is_active, room_type) VALUES (?, ?, ?, ?)",
                (name, capacity, is_active, room_type),
            )
        rows = _query(db, "SELECT id, name, capacity, is_active, room_type FROM rooms ORDER BY id ASC")
    return [dict(r) for r in rows]


def broker_create_booking(user_id: int, room_id: int, start_time: str, end_time: str, title: str) -> tuple[bool, str]:
    _ensure_broker_schema()
    db = db_paths()["broker"]
    try:
        s = datetime.fromisoformat(start_time)
        e = datetime.fromisoformat(end_time)
    except ValueError:
        return False, "Некорректный формат даты/времени"
    if e <= s:
        return False, "Время окончания должно быть позже начала"
    room = _query(db, "SELECT id, COALESCE(is_active, 1) AS is_active FROM rooms WHERE id = ? LIMIT 1", (room_id,))
    if not room:
        return False, "Ресурс не найден"
    if int(room[0]["is_active"] or 0) != 1:
        return False, "Ресурс недоступен для бронирования"
    conflicts = _query(
        db,
        """
        SELECT id
        FROM bookings
        WHERE room_id = ?
          AND start_time < ?
          AND end_time > ?
        LIMIT 1
        """,
        (room_id, e.isoformat(sep=" "), s.isoformat(sep=" ")),
    )
    if conflicts:
        return False, "Выбранный интервал пересекается с существующим бронированием"
    cols = _table_columns(db, "bookings")
    fields = ["user_id", "room_id", "start_time", "end_time", "title"]
    values: list[Any] = [user_id, room_id, s.isoformat(sep=" "), e.isoformat(sep=" "), title.strip()]
    if "reminder_sent" in cols:
        fields.append("reminder_sent")
        values.append(0)
    sql = f"INSERT INTO bookings ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
    _execute(db, sql, tuple(values))
    return True, "Бронирование создано"


def broker_bookings(user_id: int | None = None, all_rows: bool = False) -> list[dict[str, Any]]:
    _ensure_broker_schema()
    db = db_paths()["broker"]
    sql = """
    SELECT b.id, b.user_id, u.full_name, r.name AS room_name, b.start_time, b.end_time, b.title
    FROM bookings b
    LEFT JOIN users u ON u.telegram_id = b.user_id
    LEFT JOIN rooms r ON r.id = b.room_id
    """
    params: tuple[Any, ...] = ()
    if not all_rows and user_id is not None:
        sql += " WHERE b.user_id = ? AND b.end_time > datetime('now')"
        params = (user_id,)
    elif not all_rows:
        sql += " WHERE b.end_time > datetime('now')"
    sql += " ORDER BY b.start_time ASC LIMIT 500"
    rows = _query(db, sql, params)
    return [dict(r) for r in rows]


def broker_cancel_booking(booking_id: int, actor_user_id: int, actor_role: str) -> tuple[bool, str]:
    _ensure_broker_schema()
    db = db_paths()["broker"]
    row = _query(db, "SELECT user_id FROM bookings WHERE id = ? LIMIT 1", (booking_id,))
    if not row:
        return False, "Бронь не найдена"
    owner_id = int(row[0]["user_id"])
    if actor_user_id != owner_id and actor_role not in {"hr", "admin", "head"}:
        return False, "Недостаточно прав для отмены"
    affected = _execute(db, "DELETE FROM bookings WHERE id = ?", (booking_id,))
    return (affected > 0, "Бронь отменена" if affected > 0 else "Бронь не найдена")


def broker_update_role(telegram_id: int, role: str) -> tuple[bool, str]:
    _ensure_broker_schema()
    affected = _execute(db_paths()["broker"], "UPDATE users SET role = ? WHERE telegram_id = ?", (role, telegram_id))
    return (affected > 0, "Роль обновлена" if affected > 0 else "Пользователь не найден")


def broker_update_role_by_email(email: str, role: str) -> tuple[bool, str]:
    _ensure_broker_schema()
    clean_email = email.strip().lower()
    if clean_email == "":
        return False, "Укажите email"
    rows = _query(
        db_paths()["broker"],
        "SELECT telegram_id FROM web_users WHERE lower(email) = lower(?) LIMIT 1",
        (clean_email,),
    )
    if not rows:
        return False, "Пользователь с таким email не найден"
    return broker_update_role(int(rows[0]["telegram_id"]), role)


# -------------------------
# Order module (PKO/RKO)
# -------------------------

ORDER_PASSWORD = "080323"


def order_register_user(telegram_id: int, password: str, full_name: str, department: str) -> tuple[bool, str]:
    if password != ORDER_PASSWORD:
        return False, "Неверный пароль регистрации"
    _execute(
        db_paths()["order"],
        "INSERT OR REPLACE INTO users (telegram_id, full_name, department, role) VALUES (?, ?, ?, COALESCE((SELECT role FROM users WHERE telegram_id = ?), 'user'))",
        (telegram_id, full_name.strip(), department.strip(), telegram_id),
    )
    return True, "Пользователь зарегистрирован"


def order_register_web(
    access_password: str, full_name: str, department: str, email: str, account_password: str
) -> tuple[bool, str]:
    telegram_id = int(generate_telegram_id("order", email))
    ok, text = order_register_user(telegram_id, access_password, full_name, department)
    if not ok:
        return ok, text
    ok2, text2 = web_register_user("order", str(telegram_id), email, account_password)
    if not ok2:
        return False, text2
    return True, "Пользователь зарегистрирован"


def order_get_user(telegram_id: int) -> dict[str, Any] | None:
    rows = _query(
        db_paths()["order"],
        "SELECT telegram_id, full_name, department, COALESCE(role, 'user') AS role FROM users WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    )
    return dict(rows[0]) if rows else None


def _next_order_number(db: Path, key: str) -> int:
    current = _query(db, "SELECT value FROM counters WHERE key = ? LIMIT 1", (key,))
    if not current:
        _execute(db, "INSERT INTO counters (key, value) VALUES (?, ?)", (key, 1))
        return 1
    value = int(current[0]["value"]) + 1
    _execute(db, "UPDATE counters SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?", (value, key))
    return value


def order_create_request(
    telegram_id: int,
    doc_type: str,
    order_date: str,
    full_name: str,
    basis_type: str,
    contract_number: str,
    contract_date: str,
    amount: float,
) -> tuple[bool, str]:
    db = db_paths()["order"]
    key = "pko_number" if doc_type == "ПКО" else "rko_number"
    doc_number = _next_order_number(db, key)
    _execute(
        db,
        """
        INSERT INTO orders (
            doc_type, doc_number, user_id, date, full_name, basis_type, contract_number, contract_date, amount, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'на рассмотрении')
        """,
        (doc_type, doc_number, telegram_id, order_date, full_name, basis_type, contract_number, contract_date, amount),
    )
    return True, f"Заявка {doc_type} создана"


def order_requests(user_id: int | None = None) -> list[dict[str, Any]]:
    db = db_paths()["order"]
    sql = """
    SELECT id, doc_type, doc_number, user_id, date, full_name, amount, status, COALESCE(comment, '') AS comment, created_at
    FROM orders
    """
    params: tuple[Any, ...] = ()
    if user_id is not None:
        sql += " WHERE user_id = ?"
        params = (user_id,)
    sql += " ORDER BY id DESC LIMIT 500"
    return [dict(r) for r in _query(db, sql, params)]


def order_get_request(order_id: int) -> dict[str, Any] | None:
    db = db_paths()["order"]
    rows = _query(
        db,
        """
        SELECT id, doc_type, doc_number, user_id, date, full_name, basis_type, contract_number, contract_date, amount,
               status, COALESCE(comment, '') AS comment, created_at
        FROM orders
        WHERE id = ?
        LIMIT 1
        """,
        (order_id,),
    )
    return dict(rows[0]) if rows else None


def order_document_path(order_id: int) -> Path:
    docs_dir = BASE / "order-bot" / "storage" / "generated_docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    return docs_dir / f"order_{order_id}.xlsx"


def _parse_order_date(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.now()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return datetime.now()


def _month_name_ru(dt: datetime) -> str:
    months = [
        "января",
        "февраля",
        "марта",
        "апреля",
        "мая",
        "июня",
        "июля",
        "августа",
        "сентября",
        "октября",
        "ноября",
        "декабря",
    ]
    return months[dt.month - 1]


def _amount_parts(value: Any) -> tuple[int, int]:
    amount = Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    rub = int(amount)
    kop = int((amount - Decimal(rub)) * 100)
    return rub, kop


def _format_amount_ru(value: Any) -> str:
    rub, kop = _amount_parts(value)
    return f"{rub:,}".replace(",", " ") + f",{kop:02d}"


def _order_template_path(doc_type: str) -> Path:
    if str(doc_type).strip().upper() == "ПКО":
        return ORDER_DOC_TEMPLATES_DIR / "pko_template.xlsx"
    return ORDER_DOC_TEMPLATES_DIR / "rko_template.xlsx"


def _fill_rko_template(path: Path, row: dict[str, Any]) -> None:
    wb = load_workbook(path)
    ws = wb[wb.sheetnames[0]]
    dt = _parse_order_date(row.get("date"))
    amount_text = _format_amount_ru(row.get("amount"))
    contract_number = str(row.get("contract_number") or "").strip()
    contract_date = str(row.get("contract_date") or "").strip()
    basis_type = str(row.get("basis_type") or "").strip()
    basis_line = basis_type or "Основание"
    if contract_number:
        basis_line += f" № {contract_number}"
    if contract_date:
        basis_line += f" от {contract_date}"

    ws["CC11"] = int(row.get("doc_number") or 0)
    ws["CT11"] = dt.strftime("%d.%m.%Y")
    ws["CC15"] = amount_text
    full_name = str(row.get("full_name") or "")
    ws["H17"] = full_name
    ws["A17"] = full_name
    ws["K19"] = basis_line
    ws["C32"] = dt.strftime("%d")
    ws["J32"] = _month_name_ru(dt)
    ws["AC32"] = dt.strftime("%Y")
    wb.save(path)


def _fill_pko_template(path: Path, row: dict[str, Any]) -> None:
    wb = load_workbook(path)
    ws = wb[wb.sheetnames[0]]
    dt = _parse_order_date(row.get("date"))
    rub, kop = _amount_parts(row.get("amount"))
    amount_text = _format_amount_ru(row.get("amount"))
    contract_number = str(row.get("contract_number") or "").strip()
    contract_date = str(row.get("contract_date") or "").strip()
    basis_type = str(row.get("basis_type") or "").strip()
    contract_line = f"{contract_number} от {contract_date}".strip()

    ws["CX9"] = int(row.get("doc_number") or 0)
    ws["AQ13"] = int(row.get("doc_number") or 0)
    ws["BB13"] = dt.strftime("%d.%m.%Y")
    ws["CA10"] = dt.strftime("%d")
    ws["CG10"] = _month_name_ru(dt)
    ws["CW10"] = dt.strftime("%Y")
    full_name = str(row.get("full_name") or "")
    ws["G15"] = full_name
    ws["K16"] = full_name
    ws["AO19"] = amount_text
    ws["CC19"] = rub
    ws["CY19"] = kop
    ws["K23"] = basis_type
    ws["A24"] = contract_line
    wb.save(path)


def order_generate_document(order_id: int) -> tuple[bool, str, Path | None]:
    row = order_get_request(order_id)
    if not row:
        return False, "Заявка не найдена", None
    status = str(row.get("status", "")).strip().lower()
    if status != "одобрено":
        return False, "Документ доступен только для заявок со статусом 'одобрено'", None

    path = order_document_path(order_id)
    template_path = _order_template_path(str(row.get("doc_type") or ""))
    if not template_path.exists():
        return False, "Не найден шаблон документа ПКО/РКО", None

    shutil.copy2(template_path, path)
    if str(row.get("doc_type") or "").strip().upper() == "ПКО":
        _fill_pko_template(path, row)
    else:
        _fill_rko_template(path, row)
    return True, "Документ сформирован", path


def _yandex_headers() -> dict[str, str]:
    token = os.getenv("YANDEX_DISK_TOKEN", "").strip()
    if token == "":
        return {}
    return {"Authorization": f"OAuth {token}"}


def _yandex_api_json(method: str, endpoint: str, params: dict[str, Any] | None = None) -> tuple[bool, dict[str, Any] | None, str]:
    headers = _yandex_headers()
    if not headers:
        return False, None, "YANDEX_DISK_TOKEN не задан"
    url = "https://cloud-api.yandex.net/v1/disk" + endpoint
    if params:
        url += "?" + urlencode({k: str(v) for k, v in params.items() if v is not None})
    req = urlrequest.Request(url, method=method, headers=headers)
    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", "replace").strip()
            data = json.loads(raw) if raw else {}
            return True, data, "OK"
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace").strip()
        return False, None, f"HTTP {exc.code}: {raw}"
    except Exception as exc:
        return False, None, str(exc)


def _yandex_mkdirs(remote_dir: str) -> tuple[bool, str]:
    parts = [p for p in str(remote_dir).split("/") if p]
    curr = ""
    for part in parts:
        curr += "/" + part
        ok, _, text = _yandex_api_json("PUT", "/resources", {"path": f"disk:{curr}"})
        if not ok and "HTTP 409" not in text:
            return False, text
    return True, "OK"


def upload_file_to_yandex_disk(local_path: Path, remote_path: str) -> tuple[bool, str, str | None]:
    if not local_path.exists():
        return False, "Файл для загрузки не найден", None
    remote_clean = "/" + "/".join([p for p in str(remote_path).split("/") if p])
    remote_dir = "/".join(remote_clean.split("/")[:-1]) or "/"
    ok_dir, text_dir = _yandex_mkdirs(remote_dir)
    if not ok_dir:
        return False, text_dir, None

    ok_up, data_up, text_up = _yandex_api_json(
        "GET",
        "/resources/upload",
        {"path": f"disk:{remote_clean}", "overwrite": "true"},
    )
    if not ok_up or not data_up or "href" not in data_up:
        return False, f"Не удалось получить ссылку загрузки: {text_up}", None

    try:
        with local_path.open("rb") as f:
            body = f.read()
        upload_req = urlrequest.Request(
            str(data_up["href"]),
            data=body,
            method="PUT",
            headers={"Content-Type": "application/octet-stream"},
        )
        with urlrequest.urlopen(upload_req, timeout=60):
            pass
    except Exception as exc:
        return False, f"Ошибка загрузки файла: {exc}", None

    private_url = f"https://disk.yandex.ru/client/disk{quote(remote_clean)}"
    ok_pub, _, text_pub = _yandex_api_json("PUT", "/resources/publish", {"path": f"disk:{remote_clean}"})
    if not ok_pub and "HTTP 409" not in text_pub:
        if "HTTP 403" in text_pub:
            return True, "Файл загружен в Яндекс.Диск (без публичной ссылки)", private_url
        return False, f"Файл загружен, но не опубликован: {text_pub}", None

    ok_meta, data_meta, text_meta = _yandex_api_json(
        "GET",
        "/resources",
        {"path": f"disk:{remote_clean}", "fields": "public_url"},
    )
    if not ok_meta:
        return True, f"Файл загружен в Яндекс.Диск: {text_meta}", private_url
    public_url = str((data_meta or {}).get("public_url") or "").strip()
    if public_url == "":
        return True, "Файл загружен в Яндекс.Диск (без публичной ссылки)", private_url
    return True, "OK", public_url


def order_upload_document_to_yandex(order_id: int, row: dict[str, Any], local_path: Path) -> tuple[bool, str, str | None]:
    base_dir = os.getenv("YANDEX_DISK_BASE_PATH", "/Infinity").strip() or "/Infinity"
    safe_base = "/" + "/".join([p for p in base_dir.split("/") if p])
    filename = f"{row.get('doc_type', 'ORDER')}_{row.get('doc_number', order_id)}_{order_id}.xlsx"
    remote_path = f"{safe_base}/order-bot/{datetime.now().strftime('%Y-%m')}/{filename}"
    return upload_file_to_yandex_disk(local_path, remote_path)


def order_pending_requests() -> list[dict[str, Any]]:
    db = db_paths()["order"]
    has_web = bool(_table_columns(db, "web_users"))
    if has_web:
        sql = """
        SELECT o.id, o.doc_type, o.doc_number, o.user_id, COALESCE(w.email, '') AS email, o.date, o.full_name, o.amount, o.status, COALESCE(o.comment, '') AS comment, o.created_at
        FROM orders o
        LEFT JOIN web_users w ON w.telegram_id = CAST(o.user_id AS TEXT)
        WHERE o.status = 'на рассмотрении'
        ORDER BY o.id DESC
        LIMIT 500
        """
        return [dict(r) for r in _query(db, sql)]
    sql = """
    SELECT id, doc_type, doc_number, user_id, '' AS email, date, full_name, amount, status, COALESCE(comment, '') AS comment, created_at
    FROM orders
    WHERE status = 'на рассмотрении'
    ORDER BY id DESC
    LIMIT 500
    """
    return [dict(r) for r in _query(db, sql)]


def order_update_status(order_id: int, status: str, comment: str) -> tuple[bool, str]:
    affected = _execute(
        db_paths()["order"],
        "UPDATE orders SET status = ?, comment = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (status, comment, order_id),
    )
    return (affected > 0, "Статус обновлен" if affected > 0 else "Заявка не найдена")


# -------------------------
# Contracts module
# -------------------------

CONTRACT_START_NUMBER = 4988


def contracts_register_user(telegram_id: int, full_name: str, department: str) -> tuple[bool, str]:
    _execute(
        db_paths()["contracts"],
        "INSERT OR REPLACE INTO users (telegram_id, full_name, department) VALUES (?, ?, ?)",
        (telegram_id, full_name.strip(), department.strip()),
    )
    return True, "Пользователь зарегистрирован"


def contracts_register_web(full_name: str, department: str, email: str, account_password: str) -> tuple[bool, str]:
    telegram_id = int(generate_telegram_id("contracts", email))
    ok, text = contracts_register_user(telegram_id, full_name, department)
    if not ok:
        return ok, text
    ok2, text2 = web_register_user("contracts", str(telegram_id), email, account_password)
    if not ok2:
        return False, text2
    return True, "Пользователь зарегистрирован"


def contracts_get_user(telegram_id: int) -> dict[str, Any] | None:
    rows = _query(
        db_paths()["contracts"],
        "SELECT telegram_id, full_name, department FROM users WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    )
    return dict(rows[0]) if rows else None


def _next_contract_number(db: Path, department: str) -> str:
    cnt = _query(db, "SELECT COUNT(*) AS total FROM contracts")
    seq = int(cnt[0]["total"]) + CONTRACT_START_NUMBER
    return f"{seq}/{department}"


def _sync_contract_to_google_sheets(payload: dict[str, Any]) -> tuple[bool, str]:
    webhook = os.getenv("GOOGLE_SHEETS_WEBHOOK_URL", "").strip()
    if webhook == "":
        return False, "GOOGLE_SHEETS_WEBHOOK_URL не задан"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urlrequest.Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            if 200 <= status < 300:
                return True, "OK"
            return False, f"HTTP {status}"
    except urlerror.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, str(exc)


def contracts_create(telegram_id: int, form: str, address: str) -> tuple[bool, str]:
    db = db_paths()["contracts"]
    user = _query(
        db,
        "SELECT full_name, department FROM users WHERE telegram_id = ? LIMIT 1",
        (telegram_id,),
    )
    if not user:
        return False, "Пользователь не зарегистрирован"
    full_name = str(user[0]["full_name"] or "").strip()
    dep = str(user[0]["department"] or "").strip()
    number = _next_contract_number(db, dep)
    _execute(
        db,
        "INSERT INTO contracts (number, user_id, date, form, address, status) VALUES (?, ?, date('now'), ?, ?, 'Не подписан')",
        (number, telegram_id, form, address),
    )
    created_row = _query(
        db,
        "SELECT id, number, user_id, date, form, address, status FROM contracts WHERE number = ? LIMIT 1",
        (number,),
    )
    sync_ok = False
    sync_text = "Не выполнена"
    if created_row:
        row = dict(created_row[0])
        sync_ok, sync_text = _sync_contract_to_google_sheets(
            {
                "source": "web-admin",
                "event": "contract_created",
                "contract_id": row.get("id"),
                "number": row.get("number"),
                "user_id": row.get("user_id"),
                "full_name": full_name,
                "department": dep,
                "date": row.get("date"),
                "form": row.get("form"),
                "address": row.get("address"),
                "status": row.get("status"),
                "created_at": datetime.utcnow().isoformat(sep=" "),
            }
        )
    if sync_ok:
        return True, f"Договор создан: {number}. Google Sheets: синхронизировано"
    return True, f"Договор создан: {number}. Google Sheets: {sync_text}"


def contracts_list(user_id: int | None = None, only_active: bool = False) -> list[dict[str, Any]]:
    db = db_paths()["contracts"]
    sql = "SELECT id, number, user_id, date, form, address, status, created_at FROM contracts"
    clauses: list[str] = []
    params: list[Any] = []
    if user_id is not None:
        clauses.append("user_id = ?")
        params.append(user_id)
    if only_active:
        clauses.append("status = 'Не подписан'")
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT 500"
    return [dict(r) for r in _query(db, sql, tuple(params))]


def contracts_update_status(contract_id: int, status: str) -> tuple[bool, str]:
    affected = _execute(db_paths()["contracts"], "UPDATE contracts SET status = ? WHERE id = ?", (status, contract_id))
    return (affected > 0, "Статус обновлен" if affected > 0 else "Договор не найден")


def contracts_mark_signed_for_user(user_id: int, contract_id: int) -> tuple[bool, str]:
    db = db_paths()["contracts"]
    row = _query(db, "SELECT id, user_id, status FROM contracts WHERE id = ? LIMIT 1", (contract_id,))
    if not row:
        return False, "Договор не найден"
    owner_id = int(row[0]["user_id"] or 0)
    if owner_id != int(user_id):
        return False, "Недостаточно прав для изменения статуса"
    if str(row[0]["status"] or "").strip().lower() == "подписан":
        return True, "Статус уже установлен: Подписан"
    affected = _execute(db, "UPDATE contracts SET status = 'Подписан' WHERE id = ?", (contract_id,))
    return (affected > 0, "Статус изменен на Подписан" if affected > 0 else "Не удалось обновить статус")


def contracts_templates() -> list[dict[str, str]]:
    files_dir = BASE / "contract-register" / "files"
    if not files_dir.exists():
        return []
    return [{"name": p.name, "path": str(p)} for p in sorted(files_dir.glob("*.docx"))]


# -------------------------
# Docflow module
# -------------------------

DOCFLOW_PASSWORD = "080323"


def docflow_register_user(telegram_id: str, password: str, full_name: str, department_no: str) -> tuple[bool, str]:
    if password != DOCFLOW_PASSWORD:
        return False, "Неверный пароль регистрации"
    db = db_paths()["docflow"]
    cols = _table_columns(db, "users")
    values: dict[str, Any] = {
        "telegram_id": telegram_id,
        "full_name": full_name.strip(),
        "department_no": department_no.strip(),
        "role": "agent",
    }
    if "is_active" in cols:
        values["is_active"] = 0
    if "is_approved" in cols:
        values["is_approved"] = 0
    if "created_at" in cols:
        values["created_at"] = datetime.utcnow().isoformat(sep=" ")
    fields = [f for f in values.keys() if f in cols]
    sql = f"INSERT OR REPLACE INTO users ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
    _execute(db, sql, tuple(values[f] for f in fields))
    return True, "Пользователь зарегистрирован и отправлен на подтверждение РОП"


def docflow_register_web(
    access_password: str, full_name: str, department_no: str, email: str, account_password: str
) -> tuple[bool, str]:
    telegram_id = generate_telegram_id("docflow", email)
    ok, text = docflow_register_user(telegram_id, access_password, full_name, department_no)
    if not ok:
        return ok, text
    ok2, text2 = web_register_user("docflow", telegram_id, email, account_password)
    if not ok2:
        return False, text2
    return True, "Пользователь зарегистрирован и отправлен на подтверждение РОП"


def docflow_get_user(telegram_id: str) -> dict[str, Any] | None:
    db = db_paths()["docflow"]
    cols = _table_columns(db, "users")
    sql = "SELECT telegram_id, full_name, department_no, role"
    if "is_active" in cols:
        sql += ", is_active"
    if "is_approved" in cols:
        sql += ", is_approved"
    sql += " FROM users WHERE telegram_id = ? LIMIT 1"
    rows = _query(db, sql, (telegram_id,))
    return dict(rows[0]) if rows else None


def docflow_pending_users() -> list[dict[str, Any]]:
    db = db_paths()["docflow"]
    cols = _table_columns(db, "users")
    has_web = bool(_table_columns(db, "web_users"))
    select_email = "COALESCE(w.email, '') AS email," if has_web else "'' AS email,"
    join_web = "LEFT JOIN web_users w ON w.telegram_id = u.telegram_id" if has_web else ""
    sql = f"SELECT {select_email} u.telegram_id, u.full_name, u.department_no, u.role"
    if "is_approved" in cols:
        sql += ", u.is_approved"
    if "is_active" in cols:
        sql += ", u.is_active"
    sql += " FROM users u "
    if join_web:
        sql += join_web + " "
    if "is_approved" in cols:
        sql += "WHERE COALESCE(u.is_approved, 0) = 0 "
    sql += "ORDER BY u.telegram_id DESC LIMIT 300"
    return [dict(r) for r in _query(db, sql)]


def docflow_approve_user(telegram_id: str, approve: bool) -> tuple[bool, str]:
    db = db_paths()["docflow"]
    cols = _table_columns(db, "users")
    if "is_approved" in cols and "is_active" in cols:
        affected = _execute(
            db,
            "UPDATE users SET is_approved = ?, is_active = ? WHERE telegram_id = ?",
            (1 if approve else 0, 1 if approve else 0, telegram_id),
        )
    elif "is_active" in cols:
        affected = _execute(db, "UPDATE users SET is_active = ? WHERE telegram_id = ?", (1 if approve else 0, telegram_id))
    else:
        affected = _execute(db, "UPDATE users SET role = role WHERE telegram_id = ?", (telegram_id,))
    return (affected > 0, "Статус подтверждения обновлен" if affected > 0 else "Пользователь не найден")


def docflow_create_application(
    agent_telegram_id: str, deal_type: str, contract_no: str, address: str, object_type: str, head_name: str
) -> tuple[bool, str]:
    db = db_paths()["docflow"]
    user_cols = _table_columns(db, "users")
    user_select = "full_name"
    if "id" in user_cols:
        user_select = "id, full_name"
    user = _query(db, f"SELECT {user_select} FROM users WHERE telegram_id = ? LIMIT 1", (agent_telegram_id,))
    if not user:
        return False, "Сотрудник не найден"
    agent_id = user[0]["id"] if "id" in user[0].keys() else None
    agent_name = str(user[0]["full_name"] or "").strip()
    if not agent_name:
        return False, "Не заполнено ФИО сотрудника в базе"

    cols = _table_columns(db, "applications")
    values: dict[str, Any] = {
        "deal_type": deal_type,
        "contract_no": contract_no,
        "address": address,
        "object_type": object_type,
        "head_name": head_name,
        "agent_name": agent_name,
        "status": "CREATED",
        "created_at": datetime.utcnow().isoformat(sep=" "),
        "updated_at": datetime.utcnow().isoformat(sep=" "),
    }
    if agent_id is not None and "agent_id" in cols:
        values["agent_id"] = agent_id
    fields = [f for f in values.keys() if f in cols]
    if not fields:
        return False, "Таблица applications не содержит ожидаемых полей"
    sql = f"INSERT INTO applications ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
    try:
        _execute(db, sql, tuple(values[f] for f in fields))
    except sqlite3.Error as exc:
        return False, f"Ошибка создания заявки: {exc}"
    return True, "Заявка создана"


def docflow_applications() -> list[dict[str, Any]]:
    rows = _query(
        db_paths()["docflow"],
        "SELECT id, deal_type, contract_no, agent_name, status, created_at FROM applications ORDER BY id DESC LIMIT 500",
    )
    return [dict(r) for r in rows]


def docflow_applications_by_user(agent_telegram_id: str) -> list[dict[str, Any]]:
    db = db_paths()["docflow"]
    user_cols = _table_columns(db, "users")
    select_cols = "full_name"
    if "id" in user_cols:
        select_cols = "id, full_name"
    user = _query(db, f"SELECT {select_cols} FROM users WHERE telegram_id = ? LIMIT 1", (agent_telegram_id,))
    if not user:
        return []
    user_id = user[0]["id"] if "id" in user[0].keys() else None
    full_name = str(user[0]["full_name"])
    if user_id is not None:
        rows = _query(
            db,
            """
            SELECT id, deal_type, contract_no, agent_name, status, created_at
            FROM applications
            WHERE agent_id = ? OR agent_name = ?
            ORDER BY id DESC
            LIMIT 500
            """,
            (user_id, full_name),
        )
    else:
        rows = _query(
            db,
            """
            SELECT id, deal_type, contract_no, agent_name, status, created_at
            FROM applications
            WHERE agent_name = ?
            ORDER BY id DESC
            LIMIT 500
            """,
            (full_name,),
        )
    return [dict(r) for r in rows]


def docflow_update_status(app_id: int, status: str) -> tuple[bool, str]:
    affected = _execute(db_paths()["docflow"], "UPDATE applications SET status = ? WHERE id = ?", (status, app_id))
    return (affected > 0, "Статус заявки обновлен" if affected > 0 else "Заявка не найдена")

