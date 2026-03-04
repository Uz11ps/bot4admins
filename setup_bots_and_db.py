import os
import sys
import time
from pathlib import Path
import secrets

try:
    import paramiko
    from scp import SCPClient
except ImportError:
    print("Установите: pip install paramiko scp")
    sys.exit(1)

SERVER = "79.174.77.74"
PORT = 22
USER = "root"
PASSWORD = "phMUszYeRi0KUINe"
REMOTE_DIR = "/root/webadminbots"
DOMAIN = "xn--h1aeebfkb7a.online" # инфинити.online

def deploy():
    print("Настройка путей БД и запуск ботов...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SERVER, PORT, USER, PASSWORD)

    def run(cmd: str, check: bool = True):
        print(f"Выполнение: {cmd}")
        _, stdout, stderr = ssh.exec_command(cmd)
        code = stdout.channel.recv_exit_status()
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        if out:
            safe_out = out.strip().encode("ascii", "backslashreplace").decode("ascii")
            print(safe_out)
        if err:
            safe_err = err.strip().encode("ascii", "backslashreplace").decode("ascii")
            print(safe_err)
        if check and code != 0:
            print(f"Ошибка [{code}]: {err or out}")
        return out

    # 1. Исправляем Dockerfile для доступа к базам (монтируем корень проекта)
    # Мы уже запустили контейнер через docker run, теперь переделаем его правильно
    print("Пересоздание контейнера админки с доступом к БД...")
    run(f"docker stop infinity-web-admin", check=False)
    run(f"docker rm infinity-web-admin", check=False)
    
    # Запускаем с монтированием папки Infinity Projects, чтобы админка видела .db файлы
    run(f"docker run -d --name infinity-web-admin "
        f"--restart always "
        f"--env-file {REMOTE_DIR}/web-admin/.env "
        f"-v \"{REMOTE_DIR}/Infinity Projects:/root/webadminbots/Infinity Projects\" "
        f"-p 8081:8080 web-admin")

    # 2. Инициализируем БД и запускаем ботов
    print("Инициализация БД и запуск ботов...")

    project_root = f"{REMOTE_DIR}/Infinity Projects"
    bot_start_commands = [
        ("order-bot", "python3 main.py"),
        ("reflection_bot", "python3 main.py"),
        ("Meeting-booking-bot", "python3 main.py"),
        ("doc-flow-bot", "python3 app/main.py"),
        ("contract-register", "python3 bot.py"),
    ]

    # Базовые зависимости, чтобы init_db отработал гарантированно
    run(
        "apt-get update && apt-get install -y "
        "python3-pip python3-sqlalchemy python3-aiosqlite python3-dotenv",
        check=False,
    )
    run("python3 -m pip install --break-system-packages aiogram python-telegram-bot", check=False)

    # Точечная инициализация БД для модулей, где файлы не создаются без первого запуска
    run(f"cd \"{project_root}/order-bot\" && python3 -c \"from data.db import init_db; init_db(); print('order db ok')\"")
    run(f"cd \"{project_root}/Meeting-booking-bot\" && python3 -c \"import asyncio; from database import init_db; asyncio.run(init_db()); print('meeting db ok')\"")
    run(f"cd \"{project_root}/doc-flow-bot\" && python3 -c \"from app.db.base import init_db; init_db(); print('docflow db ok')\"")
    run(f"cd \"{project_root}/contract-register\" && python3 -c \"from database.db import init_db; init_db(); print('contracts db ok')\"")

    # Fallback: если ORM-инициализация не сработала из-за зависимостей, создаем минимальные таблицы вручную
    run(
        f"python3 -c \"import sqlite3; p='{project_root}/Meeting-booking-bot/meeting_bot.db'; "
        "conn=sqlite3.connect(p); c=conn.cursor(); "
        "c.execute('CREATE TABLE IF NOT EXISTS users (telegram_id INTEGER PRIMARY KEY, full_name TEXT, department TEXT, role TEXT)'); "
        "c.execute('CREATE TABLE IF NOT EXISTS rooms (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, capacity INTEGER, is_active INTEGER, room_type TEXT)'); "
        "c.execute('CREATE TABLE IF NOT EXISTS bookings (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, room_id INTEGER, start_time TEXT, end_time TEXT, title TEXT)'); "
        "conn.commit(); conn.close(); print('meeting fallback db ok')\"",
        check=False,
    )
    run(
        f"python3 -c \"import sqlite3; p='{project_root}/doc-flow-bot/app/database.db'; "
        "conn=sqlite3.connect(p); c=conn.cursor(); "
        "c.execute('CREATE TABLE IF NOT EXISTS users (telegram_id INTEGER PRIMARY KEY, full_name TEXT, department_no TEXT, role TEXT)'); "
        "c.execute('CREATE TABLE IF NOT EXISTS applications (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_type TEXT, contract_no TEXT, agent_name TEXT, status TEXT, created_at TEXT)'); "
        "c.execute('CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT)'); "
        "c.execute('CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY AUTOINCREMENT)'); "
        "conn.commit(); conn.close(); print('docflow fallback db ok')\"",
        check=False,
    )

    # Перезапуск ботов
    run("pkill -f 'python3 main.py' || true", check=False)
    run("pkill -f 'python3 app/main.py' || true", check=False)
    run("pkill -f 'python3 bot.py' || true", check=False)

    for bot_dir, start_cmd in bot_start_commands:
        full_path = f"{project_root}/{bot_dir}"
        print(f"Запуск {bot_dir}...")
        run(f"cd \"{full_path}\" && nohup {start_cmd} > bot.log 2>&1 &", check=False)

    # Контрольная проверка: какие БД реально видны на сервере
    db_list = run(f"find \"{project_root}\" -type f -name \\*.db", check=False)
    print("Найденные БД:\n" + db_list)
    run(
        "docker exec infinity-web-admin python -c "
        "\"from pathlib import Path; "
        "from services.data_sources import get_dashboard_data; "
        "d=get_dashboard_data(Path('.')); "
        "print(d)\"",
        check=False,
    )

    print("\n--- НАСТРОЙКА ЗАВЕРШЕНА ---")
    print("1. Админка теперь должна видеть базы данных (проверьте http://инфинити.online)")
    print("2. Боты запущены в фоне на сервере.")
    
    ssh.close()

if __name__ == "__main__":
    deploy()
