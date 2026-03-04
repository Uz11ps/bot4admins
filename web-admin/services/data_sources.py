from __future__ import annotations

import csv
import hashlib
import io
import sqlite3
import urllib.request
from pathlib import Path
from typing import Any


def _db_paths(project_root: Path) -> dict[str, Path]:
    # На сервере в Docker корень проекта примонтирован в /root/webadminbots
    # Внутри контейнера мы работаем в /app. 
    # Мы примонтировали Infinity Projects в /root/webadminbots/Infinity Projects
    base = Path("/root/webadminbots/Infinity Projects")
    return {
        "order": base / "order-bot" / "storage" / "orders.db",
        "reflection": base / "reflection_bot" / "data" / "journal.db",
        "meeting": base / "Meeting-booking-bot" / "meeting_bot.db",
        "broker": base / "Broker-booking-bot" / "broker_booking.db",
        "docflow": base / "doc-flow-bot" / "app" / "database.db",
        "contracts": base / "contract-register" / "contracts.db",
    }


def _query(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        return cur.fetchall()


def _count(db_path: Path, table_name: str) -> int:
    try:
        rows = _query(db_path, f"SELECT COUNT(*) AS total FROM {table_name}")
        return int(rows[0]["total"]) if rows else 0
    except sqlite3.Error:
        return 0


def _execute(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount


def _table_columns(db_path: Path, table_name: str) -> set[str]:
    rows = _query(db_path, f"PRAGMA table_info({table_name})")
    return {str(row["name"]) for row in rows}


def _table_exists(db_path: Path, table_name: str) -> bool:
    rows = _query(
        db_path,
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    return bool(rows)


def _resolve_telegram_id_by_email(db_path: Path, email: str) -> str | None:
    value = email.strip()
    if value == "" or not _table_exists(db_path, "web_users"):
        return None
    rows = _query(
        db_path,
        "SELECT telegram_id FROM web_users WHERE lower(email) = lower(?) LIMIT 1",
        (value,),
    )
    if not rows:
        return None
    return str(rows[0]["telegram_id"])


def _order_data(db_path: Path) -> dict[str, Any]:
    stats = {
        "users": _count(db_path, "users"),
        "orders": _count(db_path, "orders"),
    }
    pending = _query(db_path, "SELECT COUNT(*) AS total FROM orders WHERE status = 'на рассмотрении'")
    approved = _query(db_path, "SELECT COUNT(*) AS total FROM orders WHERE status = 'одобрено'")
    stats["pending"] = int(pending[0]["total"]) if pending else 0
    stats["approved"] = int(approved[0]["total"]) if approved else 0
    rows = _query(
        db_path,
        """
        SELECT id, doc_type, doc_number, full_name, amount, status, created_at
        FROM orders
        ORDER BY id DESC
        LIMIT 200
        """,
    )
    rows_dict = [dict(row) for row in rows]
    for row in rows_dict:
        if str(row.get("status", "")).strip().lower() == "одобрено":
            row["document"] = f"/portal/order/document/{row.get('id')}"
        else:
            row["document"] = ""
    return {
        "title": "Order Bot",
        "stats": stats,
        "columns": ["id", "doc_type", "doc_number", "full_name", "amount", "status", "created_at", "document"],
        "rows": rows_dict,
    }


def _reflection_data(db_path: Path) -> dict[str, Any]:
    stats = {
        "users": _count(db_path, "users"),
        "entries": _count(db_path, "entries"),
        "messages": _count(db_path, "messages"),
    }
    rows = _query(
        db_path,
        """
        SELECT id, user_id, date, day_rating, answer_1
        FROM entries
        ORDER BY id DESC
        LIMIT 200
        """,
    )
    return {
        "title": "Reflection Bot",
        "stats": stats,
        "columns": ["id", "user_id", "date", "day_rating", "answer_1"],
        "rows": [dict(row) for row in rows],
    }


def _meeting_data(db_path: Path) -> dict[str, Any]:
    stats = {
        "users": _count(db_path, "users"),
        "rooms": _count(db_path, "rooms"),
        "bookings": _count(db_path, "bookings"),
    }
    rows = _query(
        db_path,
        """
        SELECT b.id, b.user_id, u.full_name, r.name AS room_name, b.start_time, b.end_time, b.title
        FROM bookings b
        LEFT JOIN users u ON u.telegram_id = b.user_id
        LEFT JOIN rooms r ON r.id = b.room_id
        ORDER BY b.id DESC
        LIMIT 200
        """,
    )
    return {
        "title": "Meeting Booking Bot",
        "stats": stats,
        "columns": ["id", "user_id", "full_name", "room_name", "start_time", "end_time", "title"],
        "rows": [dict(row) for row in rows],
    }


def _broker_data(db_path: Path) -> dict[str, Any]:
    stats = {
        "users": _count(db_path, "users"),
        "rooms": _count(db_path, "rooms"),
        "bookings": _count(db_path, "bookings"),
    }
    rows = _query(
        db_path,
        """
        SELECT b.id, b.user_id, u.full_name, r.name AS room_name, b.start_time, b.end_time, b.title
        FROM bookings b
        LEFT JOIN users u ON u.telegram_id = b.user_id
        LEFT JOIN rooms r ON r.id = b.room_id
        ORDER BY b.id DESC
        LIMIT 200
        """,
    )
    return {
        "title": "Broker Booking Bot",
        "stats": stats,
        "columns": ["id", "user_id", "full_name", "room_name", "start_time", "end_time", "title"],
        "rows": [dict(row) for row in rows],
    }


def _docflow_data(db_path: Path) -> dict[str, Any]:
    stats = {
        "users": _count(db_path, "users"),
        "applications": _count(db_path, "applications"),
        "tasks": _count(db_path, "tasks"),
        "documents": _count(db_path, "documents"),
    }
    rows = _query(
        db_path,
        """
        SELECT id, deal_type, contract_no, agent_name, status, created_at
        FROM applications
        ORDER BY id DESC
        LIMIT 200
        """,
    )
    return {
        "title": "Doc Flow Bot",
        "stats": stats,
        "columns": ["id", "deal_type", "contract_no", "agent_name", "status", "created_at"],
        "rows": [dict(row) for row in rows],
    }


def _contracts_data(db_path: Path) -> dict[str, Any]:
    stats = {
        "users": _count(db_path, "users"),
        "contracts": _count(db_path, "contracts"),
    }
    unsigned = _query(db_path, "SELECT COUNT(*) AS total FROM contracts WHERE status = 'Не подписан'")
    stats["unsigned"] = int(unsigned[0]["total"]) if unsigned else 0
    rows = _query(
        db_path,
        """
        SELECT id, number, user_id, date, form, address, status, created_at
        FROM contracts
        ORDER BY id DESC
        LIMIT 200
        """,
    )
    return {
        "title": "Contract Register Bot",
        "stats": stats,
        "columns": ["id", "number", "user_id", "date", "form", "address", "status", "created_at"],
        "rows": [dict(row) for row in rows],
    }


def _order_management(db_path: Path) -> list[dict[str, Any]]:
    orders = _query(
        db_path,
        """
        SELECT id, doc_type, full_name, amount, status, COALESCE(comment, '') AS comment
        FROM orders
        ORDER BY id DESC
        LIMIT 100
        """,
    )
    if _table_exists(db_path, "web_users"):
        users = _query(
            db_path,
            """
            SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department, COALESCE(u.role, 'user') AS role
            FROM users u
            LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
            ORDER BY u.telegram_id DESC
            LIMIT 100
            """,
        )
    else:
        users = _query(
            db_path,
            """
            SELECT telegram_id, '' AS email, full_name, department, COALESCE(role, 'user') AS role
            FROM users
            ORDER BY telegram_id DESC
            LIMIT 100
            """,
        )
    return [
        {
            "title": "Order Bot - управление заявками",
            "columns": ["id", "doc_type", "full_name", "amount", "status", "comment"],
            "rows": [dict(row) for row in orders],
            "action": {
                "name": "order_update_status",
                "target_field": "id",
                "manual_target_label": "ID заявки",
                "submit_label": "Сохранить",
                "inputs": [
                    {
                        "name": "status",
                        "label": "Статус",
                        "type": "select",
                        "options": [
                            {"value": "на рассмотрении", "label": "на рассмотрении"},
                            {"value": "одобрено", "label": "одобрено"},
                            {"value": "отклонено", "label": "отклонено"},
                            {"value": "обновлено", "label": "обновлено"},
                        ],
                    },
                    {"name": "comment", "label": "Комментарий", "type": "text", "placeholder": "Комментарий"},
                ],
            },
        },
        {
            "title": "Order Bot - управление ролями пользователей",
            "columns": ["email", "full_name", "department", "role"],
            "rows": [dict(row) for row in users],
            "action": {
                "name": "order_update_user_role",
                "target_field": "telegram_id",
                "target_value_field": "telegram_id",
                "manual_target_name": "email",
                "manual_target_label": "Email",
                "submit_label": "Сменить роль",
                "inputs": [
                    {
                        "name": "role",
                        "label": "Роль",
                        "type": "select",
                        "options": [
                            {"value": "user", "label": "user"},
                            {"value": "head", "label": "head"},
                            {"value": "lawyer", "label": "lawyer"},
                            {"value": "admin", "label": "admin"},
                        ],
                    }
                ],
            },
        },
    ]


def _reflection_management(db_path: Path) -> list[dict[str, Any]]:
    users = _query(
        db_path,
        """
        SELECT user_id, display_name, department, COALESCE(is_active, 1) AS is_active
        FROM users
        ORDER BY user_id DESC
        LIMIT 100
        """,
    )
    return [
        {
            "title": "Reflection Bot - управление доступом пользователей",
            "columns": ["user_id", "display_name", "department", "is_active"],
            "rows": [dict(row) for row in users],
            "action": {
                "name": "reflection_set_active",
                "target_field": "user_id",
                "manual_target_label": "ID пользователя",
                "submit_label": "Применить",
                "inputs": [
                    {
                        "name": "is_active",
                        "label": "Активность",
                        "type": "select",
                        "options": [
                            {"value": "1", "label": "Активен"},
                            {"value": "0", "label": "Отключен"},
                        ],
                    }
                ],
            },
        }
    ]


def _meeting_management(db_path: Path) -> list[dict[str, Any]]:
    if _table_exists(db_path, "web_users"):
        users = _query(
            db_path,
            """
            SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department, COALESCE(u.role, 'user') AS role
            FROM users u
            LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
            ORDER BY u.telegram_id DESC
            LIMIT 100
            """,
        )
    else:
        users = _query(
            db_path,
            """
            SELECT telegram_id, '' AS email, full_name, department, COALESCE(role, 'user') AS role
            FROM users
            ORDER BY telegram_id DESC
            LIMIT 100
            """,
        )
    rooms = _query(
        db_path,
        """
        SELECT id, name, capacity, COALESCE(is_active, 1) AS is_active, room_type
        FROM rooms
        ORDER BY id DESC
        LIMIT 100
        """,
    )
    return [
        {
            "title": "Meeting Booking - роли пользователей",
            "columns": ["email", "full_name", "department", "role"],
            "rows": [dict(row) for row in users],
            "action": {
                "name": "meeting_update_user_role",
                "target_field": "telegram_id",
                "target_value_field": "telegram_id",
                "manual_target_name": "email",
                "manual_target_label": "Email",
                "submit_label": "Сменить роль",
                "inputs": [
                    {
                        "name": "role",
                        "label": "Роль",
                        "type": "select",
                        "options": [
                            {"value": "user", "label": "user"},
                            {"value": "hr", "label": "hr"},
                            {"value": "head", "label": "head"},
                            {"value": "admin", "label": "admin"},
                        ],
                    }
                ],
            },
        },
        {
            "title": "Meeting Booking - параметры комнат",
            "columns": ["id", "name", "capacity", "is_active", "room_type"],
            "rows": [dict(row) for row in rooms],
            "action": {
                "name": "meeting_update_room",
                "target_field": "id",
                "manual_target_label": "ID комнаты",
                "submit_label": "Обновить комнату",
                "inputs": [
                    {"name": "capacity", "label": "Вместимость", "type": "number", "min": "1"},
                    {
                        "name": "is_active",
                        "label": "Активна",
                        "type": "select",
                        "options": [
                            {"value": "1", "label": "Да"},
                            {"value": "0", "label": "Нет"},
                        ],
                    },
                ],
            },
        },
    ]


def _broker_management(db_path: Path) -> list[dict[str, Any]]:
    if _table_exists(db_path, "web_users"):
        users = _query(
            db_path,
            """
            SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department, COALESCE(u.role, 'user') AS role
            FROM users u
            LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
            ORDER BY u.telegram_id DESC
            LIMIT 100
            """,
        )
    else:
        users = _query(
            db_path,
            """
            SELECT telegram_id, '' AS email, full_name, department, COALESCE(role, 'user') AS role
            FROM users
            ORDER BY telegram_id DESC
            LIMIT 100
            """,
        )
    rooms = _query(
        db_path,
        """
        SELECT id, name, capacity, COALESCE(is_active, 1) AS is_active, room_type
        FROM rooms
        ORDER BY id DESC
        LIMIT 100
        """,
    )
    return [
        {
            "title": "Broker Booking - роли пользователей",
            "columns": ["email", "full_name", "department", "role"],
            "rows": [dict(row) for row in users],
            "action": {
                "name": "broker_update_user_role",
                "target_field": "telegram_id",
                "target_value_field": "telegram_id",
                "manual_target_name": "email",
                "manual_target_label": "Email",
                "submit_label": "Сменить роль",
                "inputs": [
                    {
                        "name": "role",
                        "label": "Роль",
                        "type": "select",
                        "options": [
                            {"value": "user", "label": "user"},
                            {"value": "hr", "label": "hr"},
                            {"value": "head", "label": "head"},
                            {"value": "admin", "label": "admin"},
                        ],
                    }
                ],
            },
        },
        {
            "title": "Broker Booking - параметры ресурсов",
            "columns": ["id", "name", "capacity", "is_active", "room_type"],
            "rows": [dict(row) for row in rooms],
            "action": {
                "name": "broker_update_room",
                "target_field": "id",
                "manual_target_label": "ID ресурса",
                "submit_label": "Обновить ресурс",
                "inputs": [
                    {"name": "capacity", "label": "Вместимость", "type": "number", "min": "1"},
                    {
                        "name": "is_active",
                        "label": "Активен",
                        "type": "select",
                        "options": [
                            {"value": "1", "label": "Да"},
                            {"value": "0", "label": "Нет"},
                        ],
                    },
                ],
            },
        },
    ]


def _docflow_management(db_path: Path) -> list[dict[str, Any]]:
    applications = _query(
        db_path,
        """
        SELECT id, deal_type, contract_no, agent_name, status, created_at
        FROM applications
        ORDER BY id DESC
        LIMIT 100
        """,
    )
    if _table_exists(db_path, "web_users"):
        users = _query(
            db_path,
            """
            SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department_no, COALESCE(u.role, 'agent') AS role
            FROM users u
            LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
            ORDER BY u.telegram_id DESC
            LIMIT 100
            """,
        )
    else:
        users = _query(
            db_path,
            """
            SELECT telegram_id, '' AS email, full_name, department_no, COALESCE(role, 'agent') AS role
            FROM users
            ORDER BY telegram_id DESC
            LIMIT 100
            """,
        )
    user_cols = _table_columns(db_path, "users")
    pending_users: list[sqlite3.Row] = []
    if "is_approved" in user_cols:
        if _table_exists(db_path, "web_users"):
            pending_users = _query(
                db_path,
                """
                SELECT u.telegram_id, COALESCE(w.email, '') AS email, u.full_name, u.department_no, COALESCE(u.role, 'agent') AS role, COALESCE(u.is_approved, 0) AS is_approved
                FROM users u
                LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
                WHERE COALESCE(u.is_approved, 0) = 0
                ORDER BY u.telegram_id DESC
                LIMIT 100
                """,
            )
        else:
            pending_users = _query(
                db_path,
                """
                SELECT telegram_id, '' AS email, full_name, department_no, COALESCE(role, 'agent') AS role, COALESCE(is_approved, 0) AS is_approved
                FROM users
                WHERE COALESCE(is_approved, 0) = 0
                ORDER BY telegram_id DESC
                LIMIT 100
                """,
            )
    return [
        {
            "title": "Doc Flow - статусы заявок",
            "columns": ["id", "deal_type", "contract_no", "agent_name", "status", "created_at"],
            "rows": [dict(row) for row in applications],
            "action": {
                "name": "docflow_update_application_status",
                "target_field": "id",
                "manual_target_label": "ID заявки",
                "submit_label": "Сменить статус",
                "inputs": [
                    {
                        "name": "status",
                        "label": "Статус",
                        "type": "select",
                        "options": [
                            {"value": "CREATED", "label": "CREATED"},
                            {"value": "RETURNED_ROP", "label": "RETURNED_ROP"},
                            {"value": "TO_LAWYER", "label": "TO_LAWYER"},
                            {"value": "LAWYER_TASK", "label": "LAWYER_TASK"},
                            {"value": "CLOSED", "label": "CLOSED"},
                        ],
                    }
                ],
            },
        },
        {
            "title": "Doc Flow - роли пользователей",
            "columns": ["email", "full_name", "department_no", "role"],
            "rows": [dict(row) for row in users],
            "action": {
                "name": "docflow_update_user_role",
                "target_field": "telegram_id",
                "target_value_field": "telegram_id",
                "manual_target_name": "email",
                "manual_target_label": "Email",
                "submit_label": "Сменить роль",
                "inputs": [
                    {
                        "name": "role",
                        "label": "Роль",
                        "type": "select",
                        "options": [
                            {"value": "agent", "label": "agent"},
                            {"value": "rop", "label": "rop"},
                            {"value": "lawyer", "label": "lawyer"},
                            {"value": "admin", "label": "admin"},
                        ],
                    }
                ],
            },
        },
        {
            "title": "Doc Flow - подтверждение сотрудников",
            "columns": ["email", "full_name", "department_no", "role", "is_approved"],
            "rows": [dict(row) for row in pending_users],
            "action": {
                "name": "docflow_approve_user",
                "target_field": "telegram_id",
                "target_value_field": "telegram_id",
                "manual_target_name": "email",
                "manual_target_label": "Email",
                "submit_label": "Применить",
                "inputs": [
                    {
                        "name": "decision",
                        "label": "Решение",
                        "type": "select",
                        "options": [
                            {"value": "approve", "label": "Подтвердить"},
                            {"value": "reject", "label": "Отклонить"},
                        ],
                    }
                ],
            },
        },
    ]


def _contracts_management(db_path: Path) -> list[dict[str, Any]]:
    contracts = _query(
        db_path,
        """
        SELECT id, number, user_id, date, form, address, status, created_at
        FROM contracts
        ORDER BY id DESC
        LIMIT 100
        """,
    )
    return [
        {
            "title": "Contract Register - управление договорами",
            "columns": ["id", "number", "user_id", "date", "form", "address", "status", "created_at"],
            "rows": [dict(row) for row in contracts],
            "action": {
                "name": "contracts_update_status",
                "target_field": "id",
                "manual_target_label": "ID договора",
                "submit_label": "Обновить статус",
                "inputs": [
                    {
                        "name": "status",
                        "label": "Статус",
                        "type": "select",
                        "options": [
                            {"value": "Не подписан", "label": "Не подписан"},
                            {"value": "Подписан", "label": "Подписан"},
                            {"value": "Аннулирован", "label": "Аннулирован"},
                        ],
                    }
                ],
            },
        }
    ]


def _module_map(project_root: Path) -> dict[str, dict[str, Any]]:
    paths = _db_paths(project_root)
    return {
        "order": {"path": paths["order"], "builder": _order_data},
        "reflection": {"path": paths["reflection"], "builder": _reflection_data},
        "meeting": {"path": paths["meeting"], "builder": _meeting_data},
        "broker": {"path": paths["broker"], "builder": _broker_data},
        "docflow": {"path": paths["docflow"], "builder": _docflow_data},
        "contracts": {"path": paths["contracts"], "builder": _contracts_data},
    }


def _management_map() -> dict[str, Any]:
    return {
        "order": _order_management,
        "reflection": _reflection_management,
        "meeting": _meeting_management,
        "broker": _broker_management,
        "docflow": _docflow_management,
        "contracts": _contracts_management,
    }


def get_module_data(project_root: Path, module_name: str) -> dict[str, Any]:
    mod = _module_map(project_root).get(module_name)
    if not mod:
        return {"exists": False, "error": "Неизвестный модуль"}
    db_path = mod["path"]
    if not db_path.exists():
        return {"exists": False, "error": f"База не найдена: {db_path}"}
    try:
        payload = mod["builder"](db_path)
        management_builder = _management_map().get(module_name)
        if management_builder:
            try:
                payload["management_blocks"] = management_builder(db_path)
            except sqlite3.Error:
                payload["management_blocks"] = []
        else:
            payload["management_blocks"] = []
        payload["exists"] = True
        return payload
    except sqlite3.Error as exc:
        return {"exists": False, "error": f"Ошибка чтения БД: {exc}"}


def _to_int(raw: Any) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _action_order_update_status(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    order_id = _to_int(payload.get("id"))
    status = str(payload.get("status", "")).strip()
    comment = str(payload.get("comment", "")).strip()
    if order_id is None or status == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE orders SET status = ?, comment = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (status, comment, order_id),
    )
    if affected <= 0:
        return False, "Заявка не найдена"
    return True, f"Статус заявки #{order_id} обновлен"


def _action_order_update_user_role(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    telegram_id = _to_int(payload.get("telegram_id"))
    if telegram_id is None:
        email = str(payload.get("email", "")).strip()
        resolved = _resolve_telegram_id_by_email(db_path, email)
        if resolved is not None:
            telegram_id = _to_int(resolved)
        elif email:
            telegram_id = _to_int(email)
    role = str(payload.get("role", "")).strip()
    if telegram_id is None or role == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE users SET role = ?, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = ?",
        (role, telegram_id),
    )
    if affected <= 0:
        return False, "Пользователь не найден"
    return True, f"Роль пользователя {telegram_id} обновлена"


def _action_reflection_set_active(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    user_id = _to_int(payload.get("user_id"))
    is_active = _to_int(payload.get("is_active"))
    if user_id is None or is_active is None or is_active not in (0, 1):
        return False, "Некорректные данные"
    affected = _execute(
        db_path,
        "UPDATE users SET is_active = ? WHERE user_id = ?",
        (is_active, user_id),
    )
    if affected <= 0:
        return False, "Пользователь не найден"
    return True, f"Статус пользователя {user_id} обновлен"


def _action_meeting_update_user_role(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    telegram_id = _to_int(payload.get("telegram_id"))
    if telegram_id is None:
        email = str(payload.get("email", "")).strip()
        resolved = _resolve_telegram_id_by_email(db_path, email)
        if resolved is not None:
            telegram_id = _to_int(resolved)
        elif email:
            telegram_id = _to_int(email)
    role = str(payload.get("role", "")).strip()
    if telegram_id is None or role == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE users SET role = ? WHERE telegram_id = ?",
        (role, telegram_id),
    )
    if affected <= 0:
        return False, "Пользователь не найден"
    return True, f"Роль пользователя {telegram_id} обновлена"


def _action_meeting_update_room(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    room_id = _to_int(payload.get("id"))
    capacity = _to_int(payload.get("capacity"))
    is_active = _to_int(payload.get("is_active"))
    if room_id is None or capacity is None or capacity <= 0 or is_active not in (0, 1):
        return False, "Некорректные параметры комнаты"
    affected = _execute(
        db_path,
        "UPDATE rooms SET capacity = ?, is_active = ? WHERE id = ?",
        (capacity, is_active, room_id),
    )
    if affected <= 0:
        return False, "Комната не найдена"
    return True, f"Комната #{room_id} обновлена"


def _action_broker_update_user_role(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    telegram_id = _to_int(payload.get("telegram_id"))
    if telegram_id is None:
        email = str(payload.get("email", "")).strip()
        resolved = _resolve_telegram_id_by_email(db_path, email)
        if resolved is not None:
            telegram_id = _to_int(resolved)
        elif email:
            telegram_id = _to_int(email)
    role = str(payload.get("role", "")).strip()
    if telegram_id is None or role == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE users SET role = ? WHERE telegram_id = ?",
        (role, telegram_id),
    )
    if affected <= 0:
        return False, "Пользователь не найден"
    return True, f"Роль пользователя {telegram_id} обновлена"


def _action_broker_update_room(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    room_id = _to_int(payload.get("id"))
    capacity = _to_int(payload.get("capacity"))
    is_active = _to_int(payload.get("is_active"))
    if room_id is None or capacity is None or capacity <= 0 or is_active not in (0, 1):
        return False, "Некорректные параметры ресурса"
    affected = _execute(
        db_path,
        "UPDATE rooms SET capacity = ?, is_active = ? WHERE id = ?",
        (capacity, is_active, room_id),
    )
    if affected <= 0:
        return False, "Ресурс не найден"
    return True, f"Ресурс #{room_id} обновлен"


def _action_docflow_update_application_status(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    app_id = _to_int(payload.get("id"))
    status = str(payload.get("status", "")).strip()
    if app_id is None or status == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE applications SET status = ? WHERE id = ?",
        (status, app_id),
    )
    if affected <= 0:
        return False, "Заявка не найдена"
    return True, f"Статус заявки #{app_id} обновлен"


def _action_docflow_update_user_role(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    telegram_id = str(payload.get("telegram_id", "")).strip()
    if telegram_id == "":
        email = str(payload.get("email", "")).strip()
        resolved = _resolve_telegram_id_by_email(db_path, email)
        if resolved is not None:
            telegram_id = resolved
        elif email != "":
            telegram_id = email
    role = str(payload.get("role", "")).strip()
    if telegram_id == "" or role == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE users SET role = ? WHERE telegram_id = ?",
        (role, telegram_id),
    )
    if affected <= 0:
        return False, "Пользователь не найден"
    return True, f"Роль пользователя {telegram_id} обновлена"


def _action_docflow_approve_user(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    telegram_id = str(payload.get("telegram_id", "")).strip()
    if telegram_id == "":
        email = str(payload.get("email", "")).strip()
        resolved = _resolve_telegram_id_by_email(db_path, email)
        if resolved is not None:
            telegram_id = resolved
        elif email != "":
            telegram_id = email
    decision = str(payload.get("decision", "")).strip().lower()
    if telegram_id == "" or decision not in {"approve", "reject"}:
        return False, "Некорректные данные"
    cols = _table_columns(db_path, "users")
    approved = 1 if decision == "approve" else 0
    active = 1 if decision == "approve" else 0
    if "is_approved" in cols and "is_active" in cols:
        affected = _execute(
            db_path,
            "UPDATE users SET is_approved = ?, is_active = ? WHERE telegram_id = ?",
            (approved, active, telegram_id),
        )
    elif "is_active" in cols:
        affected = _execute(
            db_path,
            "UPDATE users SET is_active = ? WHERE telegram_id = ?",
            (active, telegram_id),
        )
    else:
        affected = _execute(db_path, "UPDATE users SET role = role WHERE telegram_id = ?", (telegram_id,))
    if affected <= 0:
        return False, "Пользователь не найден"
    return True, "Статус сотрудника обновлен"


def _action_contracts_update_status(db_path: Path, payload: dict[str, str]) -> tuple[bool, str]:
    contract_id = _to_int(payload.get("id"))
    status = str(payload.get("status", "")).strip()
    if contract_id is None or status == "":
        return False, "Не заполнены обязательные поля"
    affected = _execute(
        db_path,
        "UPDATE contracts SET status = ? WHERE id = ?",
        (status, contract_id),
    )
    if affected <= 0:
        return False, "Договор не найден"
    return True, f"Статус договора #{contract_id} обновлен"


def apply_module_action(
    project_root: Path, module_name: str, action_name: str, payload: dict[str, str]
) -> tuple[bool, str]:
    paths = _db_paths(project_root)
    db_path = paths.get(module_name)
    if db_path is None:
        return False, "Неизвестный модуль"
    if not db_path.exists():
        return False, "База модуля не найдена"

    action_map: dict[tuple[str, str], Any] = {
        ("order", "order_update_status"): _action_order_update_status,
        ("order", "order_update_user_role"): _action_order_update_user_role,
        ("reflection", "reflection_set_active"): _action_reflection_set_active,
        ("meeting", "meeting_update_user_role"): _action_meeting_update_user_role,
        ("meeting", "meeting_update_room"): _action_meeting_update_room,
        ("broker", "broker_update_user_role"): _action_broker_update_user_role,
        ("broker", "broker_update_room"): _action_broker_update_room,
        ("docflow", "docflow_update_application_status"): _action_docflow_update_application_status,
        ("docflow", "docflow_update_user_role"): _action_docflow_update_user_role,
        ("docflow", "docflow_approve_user"): _action_docflow_approve_user,
        ("contracts", "contracts_update_status"): _action_contracts_update_status,
    }
    handler = action_map.get((module_name, action_name))
    if handler is None:
        return False, "Неизвестное действие"
    try:
        return handler(db_path, payload)
    except sqlite3.Error as exc:
        return False, f"Ошибка записи в БД: {exc}"


def get_dashboard_data(project_root: Path) -> dict[str, Any]:
    modules = _module_map(project_root)
    result: dict[str, Any] = {
        "modules": [],
        "totals": {"users": 0, "entities": 0},
    }
    for module_key, module_info in modules.items():
        db_path = module_info["path"]
        exists = db_path.exists()
        if exists:
            data = module_info["builder"](db_path)
            stats = data["stats"]
            result["totals"]["users"] += int(stats.get("users", 0))
            result["totals"]["entities"] += sum(
                int(value) for key, value in stats.items() if key != "users"
            )
            result["modules"].append(
                {
                    "key": module_key,
                    "title": data["title"],
                    "exists": True,
                    "stats": stats,
                }
            )
        else:
            result["modules"].append(
                {
                    "key": module_key,
                    "title": module_key,
                    "exists": False,
                    "stats": {},
                }
            )
    return result


def get_unified_users(project_root: Path) -> list[dict[str, Any]]:
    paths = _db_paths(project_root)
    users: list[dict[str, Any]] = []

    order_sql = (
        """
        SELECT COALESCE(w.email, '') AS email, u.full_name, u.department, u.role
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
        LIMIT 200
        """
        if _table_exists(paths["order"], "web_users")
        else "SELECT '' AS email, full_name, department, role FROM users LIMIT 200"
    )
    users.extend(
        {
            "source": "order",
            "email": row["email"],
            "full_name": row["full_name"],
            "department": row["department"],
            "role": row["role"],
        }
        for row in _query(paths["order"], order_sql)
    )
    users.extend(
        {
            "source": "reflection",
            "email": "",
            "full_name": row["display_name"],
            "department": row["department"],
            "role": "user",
        }
        for row in _query(
            paths["reflection"],
            "SELECT user_id, display_name, department FROM users LIMIT 200",
        )
    )
    meeting_sql = (
        """
        SELECT COALESCE(w.email, '') AS email, u.full_name, u.department, u.role
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
        LIMIT 200
        """
        if _table_exists(paths["meeting"], "web_users")
        else "SELECT '' AS email, full_name, department, role FROM users LIMIT 200"
    )
    users.extend(
        {
            "source": "meeting",
            "email": row["email"],
            "full_name": row["full_name"],
            "department": row["department"],
            "role": row["role"],
        }
        for row in _query(paths["meeting"], meeting_sql)
    )
    broker_sql = (
        """
        SELECT COALESCE(w.email, '') AS email, u.full_name, u.department, u.role
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
        LIMIT 200
        """
        if _table_exists(paths["broker"], "web_users")
        else "SELECT '' AS email, full_name, department, role FROM users LIMIT 200"
    )
    users.extend(
        {
            "source": "broker",
            "email": row["email"],
            "full_name": row["full_name"],
            "department": row["department"],
            "role": row["role"],
        }
        for row in _query(paths["broker"], broker_sql)
    )
    docflow_sql = (
        """
        SELECT COALESCE(w.email, '') AS email, u.full_name, u.department_no, u.role
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
        LIMIT 200
        """
        if _table_exists(paths["docflow"], "web_users")
        else "SELECT '' AS email, full_name, department_no, role FROM users LIMIT 200"
    )
    users.extend(
        {
            "source": "docflow",
            "email": row["email"],
            "full_name": row["full_name"],
            "department": row["department_no"],
            "role": row["role"],
        }
        for row in _query(paths["docflow"], docflow_sql)
    )
    contracts_sql = (
        """
        SELECT COALESCE(w.email, '') AS email, u.full_name, u.department
        FROM users u
        LEFT JOIN web_users w ON w.telegram_id = u.telegram_id
        LIMIT 200
        """
        if _table_exists(paths["contracts"], "web_users")
        else "SELECT '' AS email, full_name, department FROM users LIMIT 200"
    )
    users.extend(
        {
            "source": "contracts",
            "email": row["email"],
            "full_name": row["full_name"],
            "department": row["department"],
            "role": "user",
        }
        for row in _query(paths["contracts"], contracts_sql)
    )
    return users


def _stable_contract_user_id(full_name: str, department: str) -> int:
    seed = hashlib.sha256(f"{full_name}|{department}".encode("utf-8")).hexdigest()
    return 1_000_000_000 + (int(seed[:12], 16) % 8_000_000_000)


def _normalize_contract_status(raw: str) -> str:
    val = raw.strip()
    if val.lower() in {"отменен", "отменён", "аннулирован"}:
        return "Аннулирован"
    if val.lower() in {"подписан"}:
        return "Подписан"
    if val.lower() in {"не подписан"}:
        return "Не подписан"
    return val or "Не подписан"


def import_contracts_from_tsv(project_root: Path, tsv_text: str) -> tuple[bool, str]:
    db_path = _db_paths(project_root)["contracts"]
    if not db_path.exists():
        return False, f"База договоров не найдена: {db_path}"

    parsed = 0
    with sqlite3.connect(db_path) as conn:
        reader = csv.reader(io.StringIO(tsv_text), delimiter="\t")
        for idx, row in enumerate(reader):
            if idx == 0:
                continue
            if len(row) < 7:
                continue
            number = str(row[0]).strip()
            full_name = str(row[1]).strip()
            department = str(row[2]).strip()
            date = str(row[3]).strip()
            form = str(row[4]).strip()
            address = str(row[5]).strip()
            status = _normalize_contract_status(str(row[6]))
            if not number or number.lower().startswith("номер"):
                continue
            if not full_name:
                full_name = "Не указан"
            if not department:
                department = "0"
            user_id = _stable_contract_user_id(full_name, department)

            conn.execute(
                """
                INSERT INTO users (telegram_id, full_name, department)
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    full_name = excluded.full_name,
                    department = excluded.department
                """,
                (user_id, full_name, department),
            )
            conn.execute(
                """
                INSERT INTO contracts (number, user_id, date, form, address, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(number) DO UPDATE SET
                    user_id = excluded.user_id,
                    date = excluded.date,
                    form = excluded.form,
                    address = excluded.address,
                    status = excluded.status
                """,
                (number, user_id, date, form, address, status),
            )
            parsed += 1
        conn.commit()
    return True, f"Импорт завершен. Обработано строк: {parsed}"


def import_contracts_from_sheet_url(project_root: Path, sheet_url: str) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(sheet_url, timeout=30) as resp:
            data = resp.read().decode("utf-8", "replace")
    except Exception as exc:
        return False, f"Не удалось загрузить таблицу: {exc}"
    return import_contracts_from_tsv(project_root, data)


def approve_all_pending_docflow_users(project_root: Path) -> tuple[bool, str]:
    db_path = _db_paths(project_root)["docflow"]
    if not db_path.exists():
        return False, f"База DocFlow не найдена: {db_path}"
    cols = _table_columns(db_path, "users")
    if "is_approved" in cols and "is_active" in cols:
        affected = _execute(
            db_path,
            "UPDATE users SET is_approved = 1, is_active = 1 WHERE COALESCE(is_approved, 0) = 0",
        )
        return True, f"Подтверждено сотрудников: {max(affected, 0)}"
    if "is_active" in cols:
        affected = _execute(
            db_path,
            "UPDATE users SET is_active = 1 WHERE COALESCE(is_active, 0) = 0",
        )
        return True, f"Активировано сотрудников: {max(affected, 0)}"
    return False, "В таблице users нет полей is_approved/is_active для массового подтверждения"
