import paramiko

def get_logs():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('79.174.77.74', 22, 'root', 'phMUszYeRi0KUINe')
    
    print("--- Docker Logs (Full) ---")
    _, stdout, stderr = ssh.exec_command('docker logs infinity-web-admin')
    print(stdout.read().decode())
    print(stderr.read().decode())
    
    ssh.close()

if __name__ == "__main__":
    get_logs()
