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
DOMAIN = "xn--h1aaaawb0bm.online" # инфинити.online в Punycode

def deploy():
    print("Начало радикального деплоя...")
    sys.stdout.flush()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"Подключение к {SERVER}...")
    try:
        ssh.connect(SERVER, PORT, USER, PASSWORD, timeout=10)
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        sys.exit(1)

    def run(cmd: str, check: bool = True) -> tuple[int, str, str]:
        print(f"Выполнение: {cmd}")
        sys.stdout.flush()
        _, stdout, stderr = ssh.exec_command(cmd)
        code = stdout.channel.recv_exit_status()
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        if check and code != 0:
            print(f"Ошибка [{code}]: {cmd}\n{err or out}")
        return code, out, err

    # 1. Подготовка сервера
    print("Обновление пакетов...")
    run("apt-get update && apt-get install -y docker.io docker-compose nginx tar certbot python3-certbot-nginx", check=True)
    run("systemctl start docker && systemctl enable docker")

    # 2. Упаковка и загрузка проекта
    print("Упаковка проекта...")
    archive_name = "project.tar.gz"
    if os.name == 'nt':
        os.system(f"tar --exclude=.venv --exclude=__pycache__ --exclude=.git -czf {archive_name} .")
    else:
        os.system(f"tar --exclude='.venv' --exclude='__pycache__' --exclude='.git' -czf {archive_name} .")

    run(f"mkdir -p {REMOTE_DIR}")
    print("Загрузка архива...")
    with SCPClient(ssh.get_transport()) as scp:
        scp.put(archive_name, f"{REMOTE_DIR}/{archive_name}")
    
    if os.path.exists(archive_name):
        os.remove(archive_name)

    print("Распаковка...")
    run(f"cd {REMOTE_DIR} && tar -xzf {archive_name} && rm {archive_name}")

    # 3. Настройка .env
    google_webhook = os.getenv("GOOGLE_SHEETS_WEBHOOK_URL", "")
    yandex_token = os.getenv("YANDEX_DISK_TOKEN", "")
    yandex_base = os.getenv("YANDEX_DISK_BASE_PATH", "/Infinity")
    env_content = f"""
ADMIN_USERNAME=admin
ADMIN_PASSWORD=admin123
SESSION_SECRET_KEY={secrets.token_hex(24)}
GOOGLE_SHEETS_WEBHOOK_URL={google_webhook}
YANDEX_DISK_TOKEN={yandex_token}
YANDEX_DISK_BASE_PATH={yandex_base}
"""
    sftp = ssh.open_sftp()
    with sftp.open(f"{REMOTE_DIR}/web-admin/.env", "w") as f:
        f.write(env_content)
    sftp.close()

    # 4. Запуск Docker
    print("Перезапуск Docker контейнера...")
    run(f"docker stop infinity-web-admin", check=False)
    run(f"docker rm infinity-web-admin", check=False)
    run(f"cd {REMOTE_DIR}/web-admin && docker build -t web-admin .")
    # Исправляем монтирование: убираем лишний пробел и кавычки
    run(f"docker run -d --name infinity-web-admin --restart always --env-file {REMOTE_DIR}/web-admin/.env -v \"{REMOTE_DIR}/Infinity Projects:/root/webadminbots/Infinity Projects\" -p 8081:8080 web-admin")

    # 5. Радикальная настройка Nginx
    print("Очистка всех конфигов ISPmanager...")
    # Очищаем ВСЕ возможные места включения конфигов
    run("rm -rf /etc/nginx/vhosts/*", check=False)
    run("rm -rf /etc/nginx/vhosts-enabled/*", check=False)
    run("rm -rf /etc/nginx/sites-enabled/*", check=False)
    run("rm -rf /etc/nginx/conf.d/*", check=False)
    
    nginx_config = f"""
server {{
    listen 80;
    listen [::]:80;
    server_name {DOMAIN};

    location / {{
        proxy_pass http://127.0.0.1:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }}
}}
"""
    tmp_nginx = "/tmp/unified_admin.conf"
    sftp = ssh.open_sftp()
    with sftp.open(tmp_nginx, "w") as f:
        f.write(nginx_config)
    sftp.close()

    run(f"cp {tmp_nginx} /etc/nginx/conf.d/unified_admin.conf")
    
    print("Остановка Apache (чтобы не мешал)...")
    run("systemctl stop apache2", check=False)
    run("systemctl disable apache2", check=False)
    
    print("Перезапуск Nginx...")
    run("nginx -t && systemctl restart nginx")
    print("Выпуск/привязка SSL сертификата...")
    run(f"certbot --nginx -d {DOMAIN} --non-interactive --agree-tos -m admin@{DOMAIN} --redirect", check=False)
    run("nginx -t && systemctl restart nginx")

    print("\n--- ДЕПЛОЙ ЗАВЕРШЕН ---")
    print(f"Админка должна быть доступна по IP: http://{SERVER}")
    print(f"И по домену: https://инфинити.online")
    
    ssh.close()

if __name__ == "__main__":
    deploy()
