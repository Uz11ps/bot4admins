from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from services.data_sources import (
    approve_all_pending_docflow_users,
    apply_module_action,
    get_dashboard_data,
    import_contracts_from_sheet_url,
    import_contracts_from_tsv,
    get_module_data,
    get_unified_users,
)
from services.notifications_service import (
    add_notification,
    list_notifications,
    mark_all_read,
    mark_read,
    unread_count,
)
from services.portal_services import (
    broker_bookings,
    broker_cancel_booking,
    broker_create_booking,
    broker_get_user,
    broker_register_user,
    broker_register_web,
    broker_rooms,
    broker_update_role,
    broker_update_role_by_email,
    broker_users_with_email,
    contracts_create,
    contracts_get_user,
    contracts_list,
    contracts_mark_signed_for_user,
    contracts_register_web,
    contracts_register_user,
    contracts_templates,
    contracts_update_status,
    docflow_applications_by_user,
    docflow_applications_with_document_link,
    docflow_create_application_full,
    docflow_generate_application_document,
    docflow_get_application_details,
    docflow_get_user,
    docflow_applications,
    docflow_approve_user,
    docflow_create_application,
    docflow_questionnaire,
    docflow_upload_category_map,
    docflow_pending_users,
    docflow_register_user,
    docflow_save_application_details,
    docflow_upload_bundle_to_yandex,
    docflow_uploads_dir,
    docflow_update_status,
    meeting_get_user,
    meeting_bookings,
    meeting_cancel_booking,
    meeting_create_booking,
    meeting_register_web,
    meeting_register_user,
    meeting_rooms,
    meeting_update_role,
    meeting_users,
    meeting_users_with_email,
    meeting_update_role_by_email,
    order_get_user,
    order_create_request,
    order_generate_document,
    order_get_request,
    order_upload_document_to_yandex,
    order_register_web,
    order_register_user,
    order_requests,
    order_pending_requests,
    order_update_status,
    web_authenticate,
    web_delete_user,
    web_reset_password,
    docflow_register_web,
)


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BASE_DIR / ".env")

app = FastAPI(title="Infinity Unified Web Admin")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET_KEY", "change-me-super-secret-key"),
    max_age=60 * 60 * 12,
)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _is_authenticated(request: Request) -> bool:
    return bool(request.session.get("is_authenticated", False))


def _admin_username() -> str:
    return os.getenv("ADMIN_USERNAME", "admin")


def _admin_password() -> str:
    return os.getenv("ADMIN_PASSWORD", "admin123")


def _protected(request: Request) -> RedirectResponse | None:
    if not _is_authenticated(request):
        return RedirectResponse(url="/login", status_code=302)
    return None


def _admin_common(request: Request) -> dict[str, Any]:
    return {
        "username": request.session.get("username", ""),
        "notifications_unread": unread_count(),
    }


@app.get("/", response_class=HTMLResponse)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if _is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "title": "Вход в админку",
            "error": "",
        },
    )


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, username: str = Form(...), password: str = Form(...)) -> Any:
    if username == _admin_username() and password == _admin_password():
        request.session["is_authenticated"] = True
        request.session["username"] = username
        return RedirectResponse(url="/dashboard", status_code=302)

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "title": "Вход в админку",
            "error": "Неверный логин или пароль",
        },
    )


@app.get("/logout")
async def logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect

    data = get_dashboard_data(PROJECT_ROOT)
    users = get_unified_users(PROJECT_ROOT)
    payload = {
        "request": request,
        "title": "Единая веб-админка",
        "dashboard": data,
        "users": users[:50],
        **_admin_common(request),
    }
    return templates.TemplateResponse("dashboard.html", payload)


@app.get("/admin/panel", response_class=HTMLResponse)
async def admin_panel(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    payload = {
        "request": request,
        "title": "Панель управления",
        "contracts_sheet_url": "https://docs.google.com/spreadsheets/d/14u26fXTU1luO79sQmXUrQEUmlNIiUZskv0WTExOrzIM/edit?gid=0#gid=0",
        "message": request.query_params.get("message", ""),
        "error_message": request.query_params.get("error_message", ""),
        **_admin_common(request),
    }
    return templates.TemplateResponse("admin_panel.html", payload)


@app.get("/admin/notifications", response_class=HTMLResponse)
async def admin_notifications(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    payload = {
        "request": request,
        "title": "Уведомления",
        "notifications": list_notifications(limit=300),
        "message": request.query_params.get("message", ""),
        "error_message": request.query_params.get("error_message", ""),
        **_admin_common(request),
    }
    return templates.TemplateResponse("admin_notifications.html", payload)


@app.post("/admin/notifications/read")
async def admin_notifications_read(request: Request, notification_id: int = Form(...)) -> RedirectResponse:
    redirect = _protected(request)
    if redirect:
        return redirect
    ok = mark_read(notification_id)
    query = urlencode({"message" if ok else "error_message": "Отмечено как прочитанное" if ok else "Уведомление не найдено"})
    return RedirectResponse(url=f"/admin/notifications?{query}", status_code=302)


@app.post("/admin/notifications/read-all")
async def admin_notifications_read_all(request: Request) -> RedirectResponse:
    redirect = _protected(request)
    if redirect:
        return redirect
    count = mark_all_read()
    query = urlencode({"message": f"Отмечено прочитанными: {count}"})
    return RedirectResponse(url=f"/admin/notifications?{query}", status_code=302)


@app.post("/admin/contracts/import-sheet")
async def admin_import_contracts_sheet(request: Request) -> RedirectResponse:
    redirect = _protected(request)
    if redirect:
        return redirect
    export_url = "https://docs.google.com/spreadsheets/d/14u26fXTU1luO79sQmXUrQEUmlNIiUZskv0WTExOrzIM/export?format=tsv&gid=0"
    ok, text = import_contracts_from_sheet_url(PROJECT_ROOT, export_url)
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/admin/panel?{query}", status_code=302)


@app.post("/admin/contracts/import-text")
async def admin_import_contracts_text(request: Request, contracts_tsv: str = Form(...)) -> RedirectResponse:
    redirect = _protected(request)
    if redirect:
        return redirect
    ok, text = import_contracts_from_tsv(PROJECT_ROOT, contracts_tsv)
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/admin/panel?{query}", status_code=302)


@app.post("/admin/docflow/approve-all")
async def admin_approve_all_docflow(request: Request) -> RedirectResponse:
    redirect = _protected(request)
    if redirect:
        return redirect
    ok, text = approve_all_pending_docflow_users(PROJECT_ROOT)
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/admin/panel?{query}", status_code=302)


@app.get("/modules/{module_name}", response_class=HTMLResponse)
async def module_page(request: Request, module_name: str) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect

    data = get_module_data(PROJECT_ROOT, module_name)
    message = request.query_params.get("message", "")
    error_message = request.query_params.get("error_message", "")
    if not data["exists"]:
        return templates.TemplateResponse(
            "module.html",
            {
                "request": request,
                "title": "Модуль недоступен",
                "module_title": module_name,
                "module_key": module_name,
                "stats": {},
                "columns": [],
                "rows": [],
                "management_blocks": [],
                "error": data["error"],
                "message": message,
                "error_message": error_message,
                **_admin_common(request),
            },
        )

    return templates.TemplateResponse(
        "module.html",
        {
            "request": request,
            "title": data["title"],
            "module_title": data["title"],
            "module_key": module_name,
            "stats": data["stats"],
            "columns": data["columns"],
            "rows": data["rows"],
            "management_blocks": data.get("management_blocks", []),
            "error": "",
            "message": message,
            "error_message": error_message,
            **_admin_common(request),
        },
    )


@app.post("/modules/{module_name}/actions/{action_name}")
async def module_action(request: Request, module_name: str, action_name: str) -> RedirectResponse:
    redirect = _protected(request)
    if redirect:
        return redirect

    form = await request.form()
    payload = {key: str(value) for key, value in form.items()}
    ok, text = apply_module_action(PROJECT_ROOT, module_name, action_name, payload)
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/modules/{module_name}?{query}", status_code=302)


def _portal_redirect(module: str, ok: bool, text: str) -> RedirectResponse:
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/portal/{module}?{query}", status_code=302)


def _user_redirect(module: str, ok: bool, text: str) -> RedirectResponse:
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/user/{module}?{query}", status_code=302)


def _bot_redirect(module: str, ok: bool, text: str) -> RedirectResponse:
    query = urlencode({"message" if ok else "error_message": text})
    return RedirectResponse(url=f"/bot/{module}?{query}", status_code=302)


@app.post("/bot/{module}/reset-password")
async def bot_reset_password(module: str, email: str = Form(...), new_password: str = Form(...)) -> RedirectResponse:
    if module not in {"meeting", "broker", "order", "contracts", "docflow"}:
        return RedirectResponse(url="/bot", status_code=302)
    ok, text = web_reset_password(module, email, new_password)
    return _bot_redirect(module, ok, text)


@app.post("/bot/docflow/delete-user")
async def bot_docflow_delete_user(request: Request, email: str = Form(...)) -> RedirectResponse:
    actor_id = request.session.get("docflow_user_id")
    actor_role = str(request.session.get("docflow_user_role", "agent")).strip().lower()
    if not actor_id:
        return _bot_redirect("docflow", False, "Сначала выполните вход")
    if actor_role not in {"rop", "admin"}:
        return _bot_redirect("docflow", False, "Удаление доступно только РОП/админу")
    ok, text = web_delete_user("docflow", email)
    return _bot_redirect("docflow", ok, text)


@app.get("/bot", response_class=HTMLResponse)
async def bot_home(request: Request) -> Any:
    return templates.TemplateResponse("bot_home.html", {"request": request, "title": "Портал ботов"})


@app.get("/bot/meeting", response_class=HTMLResponse)
async def bot_meeting_page(request: Request) -> Any:
    current_id = request.session.get("meeting_user_id")
    role = request.session.get("meeting_user_role", "user")
    can_manage_roles = role in {"admin", "hr", "head"}
    return templates.TemplateResponse(
        "bot_meeting.html",
        {
            "request": request,
            "title": "Бронирование переговорных",
            "current_id": current_id,
            "current_role": role,
            "can_manage_roles": can_manage_roles,
            "rooms": meeting_rooms(),
            "my_bookings": meeting_bookings(user_id=int(current_id)) if current_id else [],
            "all_bookings": meeting_bookings(all_rows=True) if current_id else [],
            "users_with_email": meeting_users_with_email() if can_manage_roles else [],
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
        },
    )


@app.post("/bot/meeting/register")
async def bot_meeting_register(
    full_name: str = Form(...),
    department: str = Form(...),
    email: str = Form(...),
    account_password: str = Form(...),
) -> RedirectResponse:
    ok, text = meeting_register_web(full_name, department, email, account_password)
    return _bot_redirect("meeting", ok, text)


@app.post("/bot/meeting/login")
async def bot_meeting_login(request: Request, email: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    ok, text, telegram_id_str = web_authenticate("meeting", email, password)
    if not ok or telegram_id_str is None:
        return _bot_redirect("meeting", False, text)
    telegram_id = int(telegram_id_str)
    user = meeting_get_user(telegram_id)
    if not user:
        return _bot_redirect("meeting", False, "Пользователь не найден")
    request.session["meeting_user_id"] = telegram_id
    request.session["meeting_user_role"] = user.get("role", "user")
    return _bot_redirect("meeting", True, "Вход выполнен")


@app.post("/bot/meeting/create-booking")
async def bot_meeting_create_booking(
    request: Request,
    room_id: int | None = Form(None),
    start_time: str = Form(...),
    end_time: str = Form(...),
    title: str = Form(...),
) -> RedirectResponse:
    user_id = request.session.get("meeting_user_id")
    if not user_id:
        return _bot_redirect("meeting", False, "Сначала выполните вход")
    if room_id is None:
        return _bot_redirect("meeting", False, "Сначала выберите переговорную")
    try:
        ok, text = meeting_create_booking(int(user_id), room_id, start_time, end_time, title)
    except Exception:
        ok, text = False, "Ошибка при создании брони. Проверьте дату/время и попробуйте снова."
    return _bot_redirect("meeting", ok, text)


@app.post("/bot/meeting/cancel-booking")
async def bot_meeting_cancel_booking(request: Request, booking_id: int = Form(...)) -> RedirectResponse:
    user_id = request.session.get("meeting_user_id")
    role = request.session.get("meeting_user_role", "user")
    if not user_id:
        return _bot_redirect("meeting", False, "Сначала выполните вход")
    ok, text = meeting_cancel_booking(booking_id, int(user_id), str(role))
    return _bot_redirect("meeting", ok, text)


@app.post("/bot/meeting/update-role")
async def bot_meeting_update_role(request: Request, email: str = Form(...), role: str = Form(...)) -> RedirectResponse:
    user_id = request.session.get("meeting_user_id")
    actor_role = request.session.get("meeting_user_role", "user")
    if not user_id:
        return _bot_redirect("meeting", False, "Сначала выполните вход")
    if actor_role not in {"admin", "hr", "head"}:
        return _bot_redirect("meeting", False, "Недостаточно прав для смены ролей")
    ok, text = meeting_update_role_by_email(email, role)
    return _bot_redirect("meeting", ok, text)


@app.get("/bot/broker", response_class=HTMLResponse)
async def bot_broker_page(request: Request) -> Any:
    current_id = request.session.get("broker_user_id")
    role = request.session.get("broker_user_role", "user")
    can_manage_roles = role in {"admin", "hr", "head"}
    return templates.TemplateResponse(
        "bot_broker.html",
        {
            "request": request,
            "title": "Бронирование брокеров",
            "current_id": current_id,
            "current_role": role,
            "can_manage_roles": can_manage_roles,
            "rooms": broker_rooms(),
            "my_bookings": broker_bookings(user_id=int(current_id)) if current_id else [],
            "all_bookings": broker_bookings(all_rows=True) if current_id else [],
            "users_with_email": broker_users_with_email() if can_manage_roles else [],
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
        },
    )


@app.post("/bot/broker/register")
async def bot_broker_register(
    full_name: str = Form(...),
    department: str = Form(...),
    email: str = Form(...),
    account_password: str = Form(...),
) -> RedirectResponse:
    ok, text = broker_register_web(full_name, department, email, account_password)
    return _bot_redirect("broker", ok, text)


@app.post("/bot/broker/login")
async def bot_broker_login(request: Request, email: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    ok, text, telegram_id_str = web_authenticate("broker", email, password)
    if not ok or telegram_id_str is None:
        return _bot_redirect("broker", False, text)
    telegram_id = int(telegram_id_str)
    user = broker_get_user(telegram_id)
    if not user:
        return _bot_redirect("broker", False, "Пользователь не найден")
    request.session["broker_user_id"] = telegram_id
    request.session["broker_user_role"] = user.get("role", "user")
    return _bot_redirect("broker", True, "Вход выполнен")


@app.post("/bot/broker/create-booking")
async def bot_broker_create_booking(
    request: Request,
    room_id: int | None = Form(None),
    start_time: str = Form(...),
    end_time: str = Form(...),
    title: str = Form(...),
) -> RedirectResponse:
    user_id = request.session.get("broker_user_id")
    if not user_id:
        return _bot_redirect("broker", False, "Сначала выполните вход")
    if room_id is None:
        return _bot_redirect("broker", False, "Сначала выберите ресурс")
    try:
        ok, text = broker_create_booking(int(user_id), room_id, start_time, end_time, title)
    except Exception:
        ok, text = False, "Ошибка при создании брони. Проверьте дату/время и попробуйте снова."
    return _bot_redirect("broker", ok, text)


@app.post("/bot/broker/cancel-booking")
async def bot_broker_cancel_booking(request: Request, booking_id: int = Form(...)) -> RedirectResponse:
    user_id = request.session.get("broker_user_id")
    role = request.session.get("broker_user_role", "user")
    if not user_id:
        return _bot_redirect("broker", False, "Сначала выполните вход")
    ok, text = broker_cancel_booking(booking_id, int(user_id), str(role))
    return _bot_redirect("broker", ok, text)


@app.post("/bot/broker/update-role")
async def bot_broker_update_role(request: Request, email: str = Form(...), role: str = Form(...)) -> RedirectResponse:
    user_id = request.session.get("broker_user_id")
    actor_role = request.session.get("broker_user_role", "user")
    if not user_id:
        return _bot_redirect("broker", False, "Сначала выполните вход")
    if actor_role not in {"admin", "hr", "head"}:
        return _bot_redirect("broker", False, "Недостаточно прав для смены ролей")
    ok, text = broker_update_role_by_email(email, role)
    return _bot_redirect("broker", ok, text)


@app.get("/bot/order", response_class=HTMLResponse)
async def bot_order_page(request: Request) -> Any:
    current_id = request.session.get("order_user_id")
    current_role = request.session.get("order_user_role", "user")
    can_approve = current_role in {"head", "admin", "lawyer"}
    manager_dep = str(request.session.get("order_user_department") or "").strip()
    if can_approve and current_role != "admin":
        pending_requests = order_pending_requests(department=manager_dep)
    else:
        pending_requests = order_pending_requests() if can_approve else []
    approved_for_print = []
    if can_approve:
        approved_for_print = [row for row in order_requests() if str(row.get("status", "")).strip().lower() == "одобрено"]
        if current_role != "admin":
            approved_for_print = [row for row in approved_for_print if str(row.get("department") or "").strip() == manager_dep]
        approved_for_print = approved_for_print[:200]
    return templates.TemplateResponse(
        "bot_order.html",
        {
            "request": request,
            "title": "Кассовые документы",
            "current_id": current_id,
            "current_role": current_role,
            "can_approve": can_approve,
            "requests": order_requests(user_id=int(current_id)) if current_id else [],
            "pending_requests": pending_requests,
            "pending_requests_count": len(pending_requests),
            "approved_for_print": approved_for_print,
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
        },
    )


@app.post("/bot/order/register")
async def bot_order_register(
    access_password: str = Form(...),
    full_name: str = Form(...),
    department: str = Form(...),
    email: str = Form(...),
    account_password: str = Form(...),
) -> RedirectResponse:
    ok, text = order_register_web(access_password, full_name, department, email, account_password)
    return _bot_redirect("order", ok, text)


@app.post("/bot/order/login")
async def bot_order_login(request: Request, email: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    ok, text, telegram_id_str = web_authenticate("order", email, password)
    if not ok or telegram_id_str is None:
        return _bot_redirect("order", False, text)
    telegram_id = int(telegram_id_str)
    user = order_get_user(telegram_id)
    if not user:
        return _bot_redirect("order", False, "Пользователь не найден")
    request.session["order_user_id"] = telegram_id
    request.session["order_user_role"] = user.get("role", "user")
    request.session["order_user_department"] = user.get("department", "")
    return _bot_redirect("order", True, "Вход выполнен")


@app.post("/bot/order/create")
async def bot_order_create(
    request: Request,
    doc_type: str = Form(...),
    order_date: str = Form(...),
    full_name: str = Form(...),
    basis_type: str = Form(...),
    contract_number: str = Form(...),
    contract_date: str = Form(...),
    amount: float = Form(...),
) -> RedirectResponse:
    user_id = request.session.get("order_user_id")
    if not user_id:
        return _bot_redirect("order", False, "Сначала выполните вход")
    ok, text = order_create_request(
        int(user_id), doc_type, order_date, full_name, basis_type, contract_number, contract_date, amount
    )
    if ok:
        add_notification(
            category="order",
            title="Новая заявка ПКО/РКО",
            message=f"Поступила заявка {doc_type}. Автор: {full_name}, сумма: {amount}",
            link="/bot/order",
        )
    return _bot_redirect("order", ok, text)


@app.post("/bot/order/update-status")
async def bot_order_update_status(
    request: Request, order_id: int = Form(...), status: str = Form(...), comment: str = Form("")
) -> RedirectResponse:
    user_id = request.session.get("order_user_id")
    role = request.session.get("order_user_role", "user")
    if not user_id:
        return _bot_redirect("order", False, "Сначала выполните вход")
    if role not in {"head", "admin", "lawyer"}:
        return _bot_redirect("order", False, "Недостаточно прав")
    if role != "admin":
        actor = order_get_user(int(user_id))
        row_check = order_get_request(order_id)
        if not actor or not row_check:
            return _bot_redirect("order", False, "Заявка не найдена")
        dep_actor = str(actor.get("department") or "").strip()
        dep_row = str(row_check.get("department") or "").strip()
        if dep_actor == "" or dep_actor != dep_row:
            return _bot_redirect("order", False, "Можно подтверждать только заявки своего отдела")
    ok, text = order_update_status(order_id, status, comment)
    if ok and str(status).strip().lower() in {"одобрено", "approved"}:
        row = order_get_request(order_id)
        if row:
            add_notification(
                category="order_print",
                title="Документ подтвержден к печати",
                message=f"Заявка #{order_id} ({row.get('doc_type', '')}) подтверждена. Можно печатать документ.",
                link="/bot/order",
            )
    return _bot_redirect("order", ok, text)


@app.get("/bot/order/document/{order_id}")
async def bot_order_download_document(request: Request, order_id: int) -> Any:
    user_id = request.session.get("order_user_id")
    role = request.session.get("order_user_role", "user")
    if not user_id:
        return _bot_redirect("order", False, "Сначала выполните вход")

    row = order_get_request(order_id)
    if not row:
        return _bot_redirect("order", False, "Заявка не найдена")
    owner_id = int(row.get("user_id") or 0)
    if int(user_id) != owner_id and role not in {"head", "admin", "lawyer"}:
        return _bot_redirect("order", False, "Недостаточно прав для скачивания документа")

    ok, text, file_path = order_generate_document(order_id)
    if not ok or file_path is None:
        return _bot_redirect("order", False, text)
    filename = f"{row.get('doc_type', 'ORDER')}_{row.get('doc_number', order_id)}.xlsx"
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/bot/order/document/{order_id}/yadisk")
async def bot_order_open_yadisk_document(request: Request, order_id: int) -> Any:
    user_id = request.session.get("order_user_id")
    role = request.session.get("order_user_role", "user")
    if not user_id:
        return _bot_redirect("order", False, "Сначала выполните вход")
    row = order_get_request(order_id)
    if not row:
        return _bot_redirect("order", False, "Заявка не найдена")
    owner_id = int(row.get("user_id") or 0)
    if int(user_id) != owner_id and role not in {"head", "admin", "lawyer"}:
        return _bot_redirect("order", False, "Недостаточно прав для открытия документа")
    ok, text, file_path = order_generate_document(order_id)
    if not ok or file_path is None:
        return _bot_redirect("order", False, text)
    ok_u, text_u, public_url = order_upload_document_to_yandex(order_id, row, file_path)
    if not ok_u or not public_url:
        return _bot_redirect("order", False, f"Ошибка Яндекс.Диска: {text_u}")
    return RedirectResponse(url=public_url, status_code=302)


@app.get("/bot/contracts", response_class=HTMLResponse)
async def bot_contracts_page(request: Request) -> Any:
    current_id = request.session.get("contracts_user_id")
    return templates.TemplateResponse(
        "bot_contracts.html",
        {
            "request": request,
            "title": "Реестр договоров",
            "current_id": current_id,
            "mine": contracts_list(user_id=int(current_id)) if current_id else [],
            "templates_list": contracts_templates(),
            "contracts_sheet_url": "https://docs.google.com/spreadsheets/d/14u26fXTU1luO79sQmXUrQEUmlNIiUZskv0WTExOrzIM/edit?gid=0#gid=0",
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
        },
    )


@app.post("/bot/contracts/register")
async def bot_contracts_register(
    full_name: str = Form(...),
    department: str = Form(...),
    email: str = Form(...),
    account_password: str = Form(...),
) -> RedirectResponse:
    ok, text = contracts_register_web(full_name, department, email, account_password)
    return _bot_redirect("contracts", ok, text)


@app.post("/bot/contracts/login")
async def bot_contracts_login(request: Request, email: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    ok, text, telegram_id_str = web_authenticate("contracts", email, password)
    if not ok or telegram_id_str is None:
        return _bot_redirect("contracts", False, text)
    telegram_id = int(telegram_id_str)
    user = contracts_get_user(telegram_id)
    if not user:
        return _bot_redirect("contracts", False, "Пользователь не найден")
    request.session["contracts_user_id"] = telegram_id
    return _bot_redirect("contracts", True, "Вход выполнен")


@app.post("/bot/contracts/create")
async def bot_contracts_create(request: Request, form: str = Form(...), address: str = Form(...)) -> RedirectResponse:
    user_id = request.session.get("contracts_user_id")
    if not user_id:
        return _bot_redirect("contracts", False, "Сначала выполните вход")
    ok, text = contracts_create(int(user_id), form, address)
    return _bot_redirect("contracts", ok, text)


@app.post("/bot/contracts/mark-signed")
async def bot_contracts_mark_signed(
    request: Request, contract_id: int = Form(...), signed_date: str = Form("")
) -> RedirectResponse:
    user_id = request.session.get("contracts_user_id")
    if not user_id:
        return _bot_redirect("contracts", False, "Сначала выполните вход")
    ok, text = contracts_mark_signed_for_user(int(user_id), contract_id, signed_date)
    return _bot_redirect("contracts", ok, text)


@app.get("/bot/docflow", response_class=HTMLResponse)
async def bot_docflow_page(request: Request) -> Any:
    current_id = request.session.get("docflow_user_id")
    current_role = str(request.session.get("docflow_user_role", "agent")).strip().lower()
    can_manage_users = current_role in {"rop", "admin"}
    can_review = current_role in {"rop", "admin", "lawyer"}
    questionnaire = docflow_questionnaire()
    upload_categories = docflow_upload_category_map()
    manager_dep = str(request.session.get("docflow_user_department") or "").strip()
    if current_role == "rop":
        review_applications = docflow_applications_with_document_link(all_rows=True, department_no=manager_dep)
        pending_users = docflow_pending_users(department_no=manager_dep)
    elif current_role == "lawyer":
        review_applications = docflow_applications_with_document_link(all_rows=True)
        pending_users = []
    else:
        review_applications = docflow_applications_with_document_link(all_rows=True) if can_review else []
        pending_users = docflow_pending_users() if can_manage_users else []
    if can_review:
        if current_role == "lawyer":
            statuses = {"TO_LAWYER", "LAWYER_TASK"}
        else:
            statuses = {"CREATED", "RETURNED_ROP"}
        review_applications = [row for row in review_applications if str(row.get("status", "")).strip().upper() in statuses]
    return templates.TemplateResponse(
        "bot_docflow.html",
        {
            "request": request,
            "title": "Протокол и документы",
            "current_id": current_id,
            "current_role": current_role,
            "can_manage_users": can_manage_users,
            "can_review": can_review,
            "applications": docflow_applications_with_document_link(agent_telegram_id=str(current_id)) if current_id else [],
            "review_applications": review_applications,
            "pending_users": pending_users,
            "questionnaire": questionnaire,
            "upload_categories": upload_categories,
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
        },
    )


@app.post("/bot/docflow/register")
async def bot_docflow_register(
    access_password: str = Form(...),
    full_name: str = Form(...),
    department_no: str = Form(...),
    email: str = Form(...),
    account_password: str = Form(...),
) -> RedirectResponse:
    ok, text = docflow_register_web(access_password, full_name, department_no, email, account_password)
    return _bot_redirect("docflow", ok, text)


@app.post("/bot/docflow/login")
async def bot_docflow_login(request: Request, email: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    ok, text, telegram_id = web_authenticate("docflow", email, password)
    if not ok or telegram_id is None:
        return _bot_redirect("docflow", False, text)
    user = docflow_get_user(telegram_id)
    if not user:
        return _bot_redirect("docflow", False, "Пользователь не найден")
    if "is_approved" in user and int(user.get("is_approved") or 0) == 0:
        return _bot_redirect("docflow", False, "Ожидайте подтверждения РОП")
    if "is_active" in user and int(user.get("is_active") or 0) == 0:
        return _bot_redirect("docflow", False, "Учетная запись не активна")
    request.session["docflow_user_id"] = telegram_id
    request.session["docflow_user_role"] = user.get("role", "agent")
    request.session["docflow_user_department"] = user.get("department_no", "")
    return _bot_redirect("docflow", True, "Вход выполнен")


@app.post("/bot/docflow/create")
async def bot_docflow_create(
    request: Request,
    deal_type: str = Form(...),
    contract_no: str = Form(""),
    address: str = Form(""),
    object_type: str = Form(""),
    head_name: str = Form(""),
    q1: str = Form(""),
    q2: str = Form(""),
    q3: str = Form(""),
    q4: str = Form(""),
    q5: str = Form(""),
    q6: str = Form(""),
    q7: str = Form(""),
    q8: str = Form(""),
    q9: str = Form(""),
    q10: str = Form(""),
    q11: str = Form(""),
    q12: str = Form(""),
    q13: str = Form(""),
    q14: str = Form(""),
    q15: str = Form(""),
    passport_files: list[UploadFile] = File(default=[]),
    egrn_files: list[UploadFile] = File(default=[]),
    lawyer_task_files: list[UploadFile] = File(default=[]),
    other_files: list[UploadFile] = File(default=[]),
    attachments: list[UploadFile] = File(default=[]),
) -> RedirectResponse:
    agent_id = request.session.get("docflow_user_id")
    if not agent_id:
        return _bot_redirect("docflow", False, "Сначала выполните вход")
    answers = {
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "q4": q4,
        "q5": q5,
        "q6": q6,
        "q7": q7,
        "q8": q8,
        "q9": q9,
        "q10": q10,
        "q11": q11,
        "q12": q12,
        "q13": q13,
        "q14": q14,
        "q15": q15,
    }
    try:
        ok, text, app_id = docflow_create_application_full(
            str(agent_id), deal_type, contract_no, address, object_type, head_name
        )
        if not ok or app_id is None:
            return _bot_redirect("docflow", False, text)
        all_apps = docflow_applications()
        app_row = next((a for a in all_apps if int(a.get("id") or 0) == int(app_id)), {"id": app_id, "agent_name": ""})
        uploads_dir = docflow_uploads_dir(app_id)
        upload_categories = docflow_upload_category_map()
        files_by_category: dict[str, list[UploadFile]] = {
            "passport": passport_files,
            "egrn": egrn_files,
            "lawyer_task": lawyer_task_files,
            "other": other_files + attachments,
        }
        uploaded_files: dict[str, list[str]] = {k: [] for k in upload_categories.keys()}
        for category_key, files in files_by_category.items():
            folder_name = upload_categories.get(category_key, category_key)
            target_dir = uploads_dir / folder_name
            target_dir.mkdir(parents=True, exist_ok=True)
            for file in files:
                if file.filename is None or file.filename.strip() == "":
                    continue
                safe_name = file.filename.replace("/", "_").replace("\\", "_")
                target = target_dir / safe_name
                if target.exists():
                    stem = target.stem
                    suffix = target.suffix
                    idx = 1
                    while True:
                        candidate = target_dir / f"{stem}_{idx}{suffix}"
                        if not candidate.exists():
                            target = candidate
                            break
                        idx += 1
                content = await file.read()
                target.write_bytes(content)
                uploaded_files.setdefault(category_key, []).append(target.name)
        doc_path = docflow_generate_application_document(app_id, app_row, answers, uploaded_files)
        ok_save, save_text = docflow_save_application_details(app_id, answers, doc_path, uploaded_files)
        if not ok_save:
            return _bot_redirect("docflow", False, f"Заявка создана, но данные анкеты не сохранены: {save_text}")
        ok_disk, disk_text, disk_url = docflow_upload_bundle_to_yandex(app_id)
        summary = (
            f"Новая заявка #{app_id}. Тип: {deal_type}. Адрес: {address or '-'}. "
            f"Сотрудник: {app_row.get('agent_name', '')}."
        )
        if ok_disk and disk_url:
            summary += f" Я.Диск: {disk_url}"
        elif not ok_disk:
            summary += f" Я.Диск: {disk_text}"
        add_notification(
            category="docflow",
            title="Новая заявка Docflow",
            message=summary,
            link="/bot/docflow",
        )
        return _bot_redirect("docflow", True, "Заявка создана. Анкета и документ сохранены")
    except Exception as exc:
        return _bot_redirect("docflow", False, f"Ошибка создания заявки: {exc}")


@app.get("/bot/docflow/document/{app_id}")
async def bot_docflow_download_document(request: Request, app_id: int) -> Any:
    agent_id = request.session.get("docflow_user_id")
    role = request.session.get("docflow_user_role", "agent")
    if not agent_id:
        return _bot_redirect("docflow", False, "Сначала выполните вход")
    details = docflow_get_application_details(app_id)
    if not details:
        return _bot_redirect("docflow", False, "Документ по заявке не найден")
    row = next((r for r in docflow_applications() if int(r.get("id") or 0) == app_id), None)
    if row is None:
        return _bot_redirect("docflow", False, "Заявка не найдена")
    if role not in {"rop", "admin", "lawyer"}:
        user_apps = {int(r.get("id") or 0) for r in docflow_applications_by_user(str(agent_id))}
        if app_id not in user_apps:
            return _bot_redirect("docflow", False, "Недостаточно прав для просмотра документа")
    path = Path(str(details.get("document_path") or ""))
    if not path.exists():
        return _bot_redirect("docflow", False, "Файл документа не найден")
    filename = f"docflow_application_{app_id}.docx"
    return FileResponse(
        path=path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.get("/bot/docflow/yadisk/{app_id}")
async def bot_docflow_open_yadisk(request: Request, app_id: int) -> Any:
    agent_id = request.session.get("docflow_user_id")
    role = request.session.get("docflow_user_role", "agent")
    if not agent_id:
        return _bot_redirect("docflow", False, "Сначала выполните вход")
    if role not in {"rop", "admin", "lawyer"}:
        user_apps = {int(r.get("id") or 0) for r in docflow_applications_by_user(str(agent_id))}
        if app_id not in user_apps:
            return _bot_redirect("docflow", False, "Недостаточно прав")
    ok, text, url = docflow_upload_bundle_to_yandex(app_id)
    if not ok or not url:
        return _bot_redirect("docflow", False, f"Ошибка Яндекс.Диска: {text}")
    return RedirectResponse(url=url, status_code=302)


@app.post("/bot/docflow/approve-user")
async def bot_docflow_approve(request: Request, telegram_id: str = Form(...), decision: str = Form(...)) -> RedirectResponse:
    actor_id = request.session.get("docflow_user_id")
    actor_role = request.session.get("docflow_user_role", "agent")
    if not actor_id:
        return _bot_redirect("docflow", False, "Сначала выполните вход")
    if actor_role not in {"rop", "admin"}:
        return _bot_redirect("docflow", False, "Недостаточно прав")
    dep = "" if actor_role == "admin" else str(request.session.get("docflow_user_department") or "").strip()
    ok, text = docflow_approve_user(telegram_id, decision == "approve", dep)
    return _bot_redirect("docflow", ok, text)


@app.post("/bot/docflow/status")
async def bot_docflow_status(request: Request, app_id: int = Form(...), status: str = Form(...)) -> RedirectResponse:
    actor_id = request.session.get("docflow_user_id")
    actor_role = request.session.get("docflow_user_role", "agent")
    if not actor_id:
        return _bot_redirect("docflow", False, "Сначала выполните вход")
    if actor_role not in {"rop", "admin", "lawyer"}:
        return _bot_redirect("docflow", False, "Недостаточно прав")
    dep = ""
    if actor_role == "rop":
        dep = str(request.session.get("docflow_user_department") or "").strip()
    ok, text = docflow_update_status(app_id, status, dep)
    if ok and str(status).strip().upper() == "TO_LAWYER":
        app_row = next((a for a in docflow_applications() if int(a.get("id") or 0) == int(app_id)), None)
        if app_row:
            add_notification(
                category="docflow_lawyer",
                title="Заявка направлена юристу",
                message=f"Заявка #{app_id} направлена юристу. Тип: {app_row.get('deal_type', '')}, агент: {app_row.get('agent_name', '')}",
                link="/bot/docflow",
            )
    return _bot_redirect("docflow", ok, text)

@app.get("/portal", response_class=HTMLResponse)
async def portal_home(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        "portal_home.html",
        {"request": request, "title": "Портал модулей", **_admin_common(request)},
    )


@app.get("/user", response_class=HTMLResponse)
async def user_home(request: Request) -> Any:
    return templates.TemplateResponse(
        "user_home.html",
        {"request": request, "title": "Пользовательский портал", "username": request.session.get("username", "")},
    )


@app.get("/user/logout")
async def user_logout(request: Request) -> RedirectResponse:
    for key in [
        "meeting_user_id",
        "broker_user_id",
        "order_user_id",
        "contracts_user_id",
        "docflow_user_id",
        "order_user_department",
        "docflow_user_department",
    ]:
        request.session.pop(key, None)
    return RedirectResponse(url="/user", status_code=302)


@app.post("/user/meeting/login")
async def user_meeting_login(request: Request, telegram_id: int = Form(...)) -> RedirectResponse:
    user = meeting_get_user(telegram_id)
    if not user:
        return _user_redirect("meeting", False, "Пользователь не найден. Сначала зарегистрируйтесь.")
    request.session["meeting_user_id"] = telegram_id
    request.session["meeting_user_role"] = user.get("role", "user")
    return _user_redirect("meeting", True, "Вход выполнен")


@app.post("/user/order/login")
async def user_order_login(request: Request, telegram_id: int = Form(...), password: str = Form(...)) -> RedirectResponse:
    user = order_get_user(telegram_id)
    if not user:
        return _user_redirect("order", False, "Пользователь не найден. Сначала зарегистрируйтесь.")
    if password != "080323":
        return _user_redirect("order", False, "Неверный пароль")
    request.session["order_user_id"] = telegram_id
    return _user_redirect("order", True, "Вход выполнен")


@app.post("/user/contracts/login")
async def user_contracts_login(request: Request, telegram_id: int = Form(...)) -> RedirectResponse:
    user = contracts_get_user(telegram_id)
    if not user:
        return _user_redirect("contracts", False, "Пользователь не найден. Сначала зарегистрируйтесь.")
    request.session["contracts_user_id"] = telegram_id
    return _user_redirect("contracts", True, "Вход выполнен")


@app.post("/user/docflow/login")
async def user_docflow_login(request: Request, telegram_id: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    user = docflow_get_user(telegram_id)
    if not user:
        return _user_redirect("docflow", False, "Пользователь не найден. Сначала зарегистрируйтесь.")
    if password != "080323":
        return _user_redirect("docflow", False, "Неверный пароль")
    if "is_approved" in user and int(user.get("is_approved") or 0) == 0:
        return _user_redirect("docflow", False, "Ожидайте подтверждения РОП")
    if "is_active" in user and int(user.get("is_active") or 0) == 0:
        return _user_redirect("docflow", False, "Учетная запись не активна")
    request.session["docflow_user_id"] = telegram_id
    return _user_redirect("docflow", True, "Вход выполнен")


@app.get("/user/meeting", response_class=HTMLResponse)
async def user_meeting_page(request: Request) -> Any:
    current_id = request.session.get("meeting_user_id")
    role = request.session.get("meeting_user_role", "user")
    bookings = meeting_bookings(user_id=int(current_id)) if current_id else []
    all_bookings = meeting_bookings(all_rows=True) if role in {"hr", "admin"} else []
    return templates.TemplateResponse(
        "user_meeting.html",
        {
            "request": request,
            "title": "Пользователь: бронирование переговорных",
            "current_id": current_id,
            "current_role": role,
            "rooms": meeting_rooms(),
            "my_bookings": bookings,
            "all_bookings": all_bookings,
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            "username": request.session.get("username", ""),
        },
    )


@app.post("/user/meeting/register")
async def user_meeting_register(telegram_id: int = Form(...), full_name: str = Form(...), department: str = Form(...)) -> RedirectResponse:
    ok, text = meeting_register_user(telegram_id, full_name, department)
    return _user_redirect("meeting", ok, text)


@app.post("/user/meeting/create-booking")
async def user_meeting_create_booking(
    request: Request, room_id: int = Form(...), start_time: str = Form(...), end_time: str = Form(...), title: str = Form(...)
) -> RedirectResponse:
    user_id = request.session.get("meeting_user_id")
    if not user_id:
        return _user_redirect("meeting", False, "Сначала выполните вход")
    ok, text = meeting_create_booking(int(user_id), room_id, start_time, end_time, title)
    return _user_redirect("meeting", ok, text)


@app.post("/user/meeting/cancel-booking")
async def user_meeting_cancel_booking(request: Request, booking_id: int = Form(...)) -> RedirectResponse:
    user_id = request.session.get("meeting_user_id")
    role = request.session.get("meeting_user_role", "user")
    if not user_id:
        return _user_redirect("meeting", False, "Сначала выполните вход")
    ok, text = meeting_cancel_booking(booking_id, int(user_id), str(role))
    return _user_redirect("meeting", ok, text)


@app.get("/user/order", response_class=HTMLResponse)
async def user_order_page(request: Request) -> Any:
    current_id = request.session.get("order_user_id")
    requests_data = order_requests(user_id=int(current_id)) if current_id else []
    return templates.TemplateResponse(
        "user_order.html",
        {
            "request": request,
            "title": "Пользователь: ПКО/РКО",
            "current_id": current_id,
            "requests": requests_data,
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            "username": request.session.get("username", ""),
        },
    )


@app.post("/user/order/register")
async def user_order_register(
    telegram_id: int = Form(...), password: str = Form(...), full_name: str = Form(...), department: str = Form(...)
) -> RedirectResponse:
    ok, text = order_register_user(telegram_id, password, full_name, department)
    return _user_redirect("order", ok, text)


@app.post("/user/order/create")
async def user_order_create(
    request: Request,
    doc_type: str = Form(...),
    order_date: str = Form(...),
    full_name: str = Form(...),
    basis_type: str = Form(...),
    contract_number: str = Form(...),
    contract_date: str = Form(...),
    amount: float = Form(...),
) -> RedirectResponse:
    user_id = request.session.get("order_user_id")
    if not user_id:
        return _user_redirect("order", False, "Сначала выполните вход")
    ok, text = order_create_request(
        int(user_id), doc_type, order_date, full_name, basis_type, contract_number, contract_date, amount
    )
    if ok:
        add_notification(
            category="order",
            title="Новая заявка ПКО/РКО",
            message=f"Поступила заявка {doc_type}. Автор: {full_name}, сумма: {amount}",
            link="/portal/order",
        )
    return _user_redirect("order", ok, text)


@app.get("/user/contracts", response_class=HTMLResponse)
async def user_contracts_page(request: Request) -> Any:
    current_id = request.session.get("contracts_user_id")
    mine = contracts_list(user_id=int(current_id)) if current_id else []
    active = contracts_list(user_id=int(current_id), only_active=True) if current_id else []
    return templates.TemplateResponse(
        "user_contracts.html",
        {
            "request": request,
            "title": "Пользователь: реестр договоров",
            "current_id": current_id,
            "mine": mine,
            "active": active,
            "templates_list": contracts_templates(),
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            "username": request.session.get("username", ""),
        },
    )


@app.post("/user/contracts/register")
async def user_contracts_register(telegram_id: int = Form(...), full_name: str = Form(...), department: str = Form(...)) -> RedirectResponse:
    ok, text = contracts_register_user(telegram_id, full_name, department)
    return _user_redirect("contracts", ok, text)


@app.post("/user/contracts/create")
async def user_contracts_create(request: Request, form: str = Form(...), address: str = Form(...)) -> RedirectResponse:
    user_id = request.session.get("contracts_user_id")
    if not user_id:
        return _user_redirect("contracts", False, "Сначала выполните вход")
    ok, text = contracts_create(int(user_id), form, address)
    return _user_redirect("contracts", ok, text)


@app.get("/user/docflow", response_class=HTMLResponse)
async def user_docflow_page(request: Request) -> Any:
    current_id = request.session.get("docflow_user_id")
    apps = docflow_applications_by_user(str(current_id)) if current_id else []
    return templates.TemplateResponse(
        "user_docflow.html",
        {
            "request": request,
            "title": "Пользователь: протокол и документы",
            "current_id": current_id,
            "applications": apps,
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            "username": request.session.get("username", ""),
        },
    )


@app.post("/user/docflow/register")
async def user_docflow_register(
    telegram_id: str = Form(...), password: str = Form(...), full_name: str = Form(...), department_no: str = Form(...)
) -> RedirectResponse:
    ok, text = docflow_register_user(telegram_id, password, full_name, department_no)
    return _user_redirect("docflow", ok, text)


@app.post("/user/docflow/create")
async def user_docflow_create(
    request: Request,
    deal_type: str = Form(...),
    contract_no: str = Form(""),
    address: str = Form(""),
    object_type: str = Form(""),
    head_name: str = Form(""),
) -> RedirectResponse:
    agent_id = request.session.get("docflow_user_id")
    if not agent_id:
        return _user_redirect("docflow", False, "Сначала выполните вход")
    ok, text = docflow_create_application(str(agent_id), deal_type, contract_no, address, object_type, head_name)
    if ok:
        add_notification(
            category="docflow",
            title="Новая заявка Docflow",
            message=f"Поступила заявка по типу сделки: {deal_type}",
            link="/portal/docflow",
        )
    return _user_redirect("docflow", ok, text)


@app.get("/portal/meeting", response_class=HTMLResponse)
async def portal_meeting(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        "portal_meeting.html",
        {
            "request": request,
            "title": "Модуль бронирования переговорных",
            "rooms": meeting_rooms(),
            "users": meeting_users(),
            "my_bookings": meeting_bookings(),
            "all_bookings": meeting_bookings(all_rows=True),
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            **_admin_common(request),
        },
    )


@app.post("/portal/meeting/register")
async def portal_meeting_register(
    telegram_id: int = Form(...), full_name: str = Form(...), department: str = Form(...)
) -> RedirectResponse:
    ok, text = meeting_register_user(telegram_id, full_name, department)
    return _portal_redirect("meeting", ok, text)


@app.post("/portal/meeting/create-booking")
async def portal_meeting_create_booking(
    user_id: int = Form(...),
    room_id: int = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    title: str = Form(...),
) -> RedirectResponse:
    ok, text = meeting_create_booking(user_id, room_id, start_time, end_time, title)
    return _portal_redirect("meeting", ok, text)


@app.post("/portal/meeting/cancel-booking")
async def portal_meeting_cancel_booking(
    booking_id: int = Form(...), actor_user_id: int = Form(...), actor_role: str = Form(...)
) -> RedirectResponse:
    ok, text = meeting_cancel_booking(booking_id, actor_user_id, actor_role)
    return _portal_redirect("meeting", ok, text)


@app.post("/portal/meeting/update-role")
async def portal_meeting_update_role(telegram_id: int = Form(...), role: str = Form(...)) -> RedirectResponse:
    ok, text = meeting_update_role(telegram_id, role)
    return _portal_redirect("meeting", ok, text)


@app.get("/portal/broker", response_class=HTMLResponse)
async def portal_broker(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        "portal_broker.html",
        {
            "request": request,
            "title": "Модуль бронирования брокеров",
            "rooms": broker_rooms(),
            "all_bookings": broker_bookings(all_rows=True),
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            **_admin_common(request),
        },
    )


@app.post("/portal/broker/register")
async def portal_broker_register(
    telegram_id: int = Form(...), full_name: str = Form(...), department: str = Form(...)
) -> RedirectResponse:
    ok, text = broker_register_user(telegram_id, full_name, department)
    return _portal_redirect("broker", ok, text)


@app.post("/portal/broker/create-booking")
async def portal_broker_create_booking(
    user_id: int = Form(...),
    room_id: int = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    title: str = Form(...),
) -> RedirectResponse:
    ok, text = broker_create_booking(user_id, room_id, start_time, end_time, title)
    return _portal_redirect("broker", ok, text)


@app.post("/portal/broker/cancel-booking")
async def portal_broker_cancel_booking(
    booking_id: int = Form(...), actor_user_id: int = Form(...), actor_role: str = Form(...)
) -> RedirectResponse:
    ok, text = broker_cancel_booking(booking_id, actor_user_id, actor_role)
    return _portal_redirect("broker", ok, text)


@app.post("/portal/broker/update-role")
async def portal_broker_update_role(telegram_id: int = Form(...), role: str = Form(...)) -> RedirectResponse:
    ok, text = broker_update_role(telegram_id, role)
    return _portal_redirect("broker", ok, text)


@app.get("/portal/order", response_class=HTMLResponse)
async def portal_order(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        "portal_order.html",
        {
            "request": request,
            "title": "Модуль кассовых документов (ПКО/РКО)",
            "requests": order_requests(),
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            **_admin_common(request),
        },
    )


@app.post("/portal/order/register")
async def portal_order_register(
    telegram_id: int = Form(...), password: str = Form(...), full_name: str = Form(...), department: str = Form(...)
) -> RedirectResponse:
    ok, text = order_register_user(telegram_id, password, full_name, department)
    return _portal_redirect("order", ok, text)


@app.post("/portal/order/create")
async def portal_order_create(
    telegram_id: int = Form(...),
    doc_type: str = Form(...),
    order_date: str = Form(...),
    full_name: str = Form(...),
    basis_type: str = Form(...),
    contract_number: str = Form(...),
    contract_date: str = Form(...),
    amount: float = Form(...),
) -> RedirectResponse:
    ok, text = order_create_request(
        telegram_id, doc_type, order_date, full_name, basis_type, contract_number, contract_date, amount
    )
    if ok:
        add_notification(
            category="order",
            title="Новая заявка ПКО/РКО",
            message=f"Поступила заявка {doc_type}. Автор: {full_name}, сумма: {amount}",
            link="/portal/order",
        )
    return _portal_redirect("order", ok, text)


@app.post("/portal/order/status")
async def portal_order_status(order_id: int = Form(...), status: str = Form(...), comment: str = Form("")) -> RedirectResponse:
    ok, text = order_update_status(order_id, status, comment)
    if ok and str(status).strip().lower() in {"одобрено", "approved"}:
        row = order_get_request(order_id)
        if row:
            add_notification(
                category="order_print",
                title="Документ подтвержден к печати",
                message=f"Заявка #{order_id} ({row.get('doc_type', '')}) подтверждена. Можно печатать документ.",
                link="/portal/order",
            )
    return _portal_redirect("order", ok, text)


@app.get("/portal/order/document/{order_id}")
async def portal_order_download_document(request: Request, order_id: int) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    row = order_get_request(order_id)
    if not row:
        return _portal_redirect("order", False, "Заявка не найдена")
    ok, text, file_path = order_generate_document(order_id)
    if not ok or file_path is None:
        return _portal_redirect("order", False, text)
    filename = f"{row.get('doc_type', 'ORDER')}_{row.get('doc_number', order_id)}.xlsx"
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/portal/order/document/{order_id}/yadisk")
async def portal_order_open_yadisk_document(request: Request, order_id: int) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    row = order_get_request(order_id)
    if not row:
        return _portal_redirect("order", False, "Заявка не найдена")
    ok, text, file_path = order_generate_document(order_id)
    if not ok or file_path is None:
        return _portal_redirect("order", False, text)
    ok_u, text_u, public_url = order_upload_document_to_yandex(order_id, row, file_path)
    if not ok_u or not public_url:
        return _portal_redirect("order", False, f"Ошибка Яндекс.Диска: {text_u}")
    return RedirectResponse(url=public_url, status_code=302)


@app.get("/portal/contracts", response_class=HTMLResponse)
async def portal_contracts(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        "portal_contracts.html",
        {
            "request": request,
            "title": "Модуль реестра договоров",
            "contracts": contracts_list(),
            "active_contracts": contracts_list(only_active=True),
            "templates_list": contracts_templates(),
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            **_admin_common(request),
        },
    )


@app.post("/portal/contracts/register")
async def portal_contracts_register(
    telegram_id: int = Form(...), full_name: str = Form(...), department: str = Form(...)
) -> RedirectResponse:
    ok, text = contracts_register_user(telegram_id, full_name, department)
    return _portal_redirect("contracts", ok, text)


@app.post("/portal/contracts/create")
async def portal_contracts_create(telegram_id: int = Form(...), form: str = Form(...), address: str = Form(...)) -> RedirectResponse:
    ok, text = contracts_create(telegram_id, form, address)
    return _portal_redirect("contracts", ok, text)


@app.post("/portal/contracts/status")
async def portal_contracts_status(contract_id: int = Form(...), status: str = Form(...)) -> RedirectResponse:
    ok, text = contracts_update_status(contract_id, status)
    return _portal_redirect("contracts", ok, text)


@app.get("/portal/contracts/template")
async def portal_contract_template(path: str) -> Any:
    file_path = Path(path)
    if not file_path.exists() or not str(file_path).startswith("/root/webadminbots/Infinity Projects/contract-register/files"):
        return RedirectResponse(url="/portal/contracts?" + urlencode({"error_message": "Файл шаблона не найден"}), status_code=302)
    return FileResponse(path=file_path, filename=file_path.name, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document")


@app.get("/portal/docflow", response_class=HTMLResponse)
async def portal_docflow(request: Request) -> Any:
    redirect = _protected(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        "portal_docflow.html",
        {
            "request": request,
            "title": "Модуль протокола и документов",
            "pending_users": docflow_pending_users(),
            "applications": docflow_applications(),
            "message": request.query_params.get("message", ""),
            "error_message": request.query_params.get("error_message", ""),
            **_admin_common(request),
        },
    )


@app.post("/portal/docflow/register")
async def portal_docflow_register(
    telegram_id: str = Form(...), password: str = Form(...), full_name: str = Form(...), department_no: str = Form(...)
) -> RedirectResponse:
    ok, text = docflow_register_user(telegram_id, password, full_name, department_no)
    return _portal_redirect("docflow", ok, text)


@app.post("/portal/docflow/approve-user")
async def portal_docflow_approve_user(telegram_id: str = Form(...), decision: str = Form(...)) -> RedirectResponse:
    ok, text = docflow_approve_user(telegram_id, decision == "approve")
    return _portal_redirect("docflow", ok, text)


@app.post("/portal/docflow/create")
async def portal_docflow_create(
    agent_telegram_id: str = Form(...),
    deal_type: str = Form(...),
    contract_no: str = Form(""),
    address: str = Form(""),
    object_type: str = Form(""),
    head_name: str = Form(""),
) -> RedirectResponse:
    ok, text = docflow_create_application(agent_telegram_id, deal_type, contract_no, address, object_type, head_name)
    if ok:
        add_notification(
            category="docflow",
            title="Новая заявка Docflow",
            message=f"Поступила заявка по типу сделки: {deal_type}",
            link="/portal/docflow",
        )
    return _portal_redirect("docflow", ok, text)


@app.post("/portal/docflow/status")
async def portal_docflow_status(app_id: int = Form(...), status: str = Form(...)) -> RedirectResponse:
    ok, text = docflow_update_status(app_id, status)
    if ok and str(status).strip().upper() == "TO_LAWYER":
        app_row = next((a for a in docflow_applications() if int(a.get("id") or 0) == int(app_id)), None)
        if app_row:
            add_notification(
                category="docflow_lawyer",
                title="Заявка направлена юристу",
                message=f"Заявка #{app_id} направлена юристу. Тип: {app_row.get('deal_type', '')}, агент: {app_row.get('agent_name', '')}",
                link="/portal/docflow",
            )
    return _portal_redirect("docflow", ok, text)
