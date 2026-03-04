import paramiko

def check_nginx():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('79.174.77.74', 22, 'root', 'phMUszYeRi0KUINe')
    
    # Ищем все файлы баз данных (включая скрытые и вложенные)
    cmd = 'find /root/webadminbots/Infinity\ Projects -name "*.db"'
    _, stdout, _ = ssh.exec_command(cmd)
    print("Найденные БД на сервере:")
    print(stdout.read().decode())
    
    # Проверяем наличие файлов в папках
    for bot in ["order-bot/storage", "Meeting-booking-bot", "doc-flow-bot/app/db", "contract-register/database"]:
        print(f"--- Файлы в {bot} ---")
        cmd = f'ls -F /root/webadminbots/Infinity\ Projects/{bot}/'
        _, stdout, _ = ssh.exec_command(cmd)
        print(stdout.read().decode())
    
    # Проверяем активные процессы nginx
    _, stdout, _ = ssh.exec_command('ps aux | grep nginx')
    print("Процессы Nginx:")
    print(stdout.read().decode())

    # Проверяем кто слушает 80 порт
    _, stdout, _ = ssh.exec_command('ss -tulpn | grep :80')
    print("Кто слушает 80 порт (ss):")
    print(stdout.read().decode())
    
    # Проверяем статус сервиса
    _, stdout, _ = ssh.exec_command('systemctl status nginx')
    print("Статус Nginx:")
    print(stdout.read().decode())
    
    ssh.close()

if __name__ == "__main__":
    check_nginx()
