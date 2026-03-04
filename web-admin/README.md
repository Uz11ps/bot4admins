# Unified Web Admin

Единая веб-админка для 5 ботов:

- `order-bot`
- `reflection_bot`
- `Meeting-booking-bot`
- `doc-flow-bot`
- `contract-register`

## Локальный запуск

```bash
cd web-admin
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app:app --host 0.0.0.0 --port 8080
```

## Прод-деплой (Linux)

```bash
cd /opt/webadminbots/web-admin
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app:app --host 0.0.0.0 --port 8080
```

## Systemd unit (рекомендуется)

`/etc/systemd/system/infinity-web-admin.service`

```ini
[Unit]
Description=Infinity Unified Web Admin
After=network.target

[Service]
User=root
WorkingDirectory=/opt/webadminbots/web-admin
Environment="PYTHONUNBUFFERED=1"
ExecStart=/opt/webadminbots/web-admin/.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Запуск сервиса:

```bash
systemctl daemon-reload
systemctl enable infinity-web-admin
systemctl restart infinity-web-admin
systemctl status infinity-web-admin
```
