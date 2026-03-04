import paramiko

def debug_502():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('79.174.77.74', 22, 'root', 'phMUszYeRi0KUINe')
    
    print("--- Docker PS ---")
    _, stdout, _ = ssh.exec_command('docker ps')
    print(stdout.read().decode())
    
    print("--- Docker Logs (infinity-web-admin) ---")
    _, stdout, _ = ssh.exec_command('docker logs --tail 20 infinity-web-admin')
    print(stdout.read().decode())
    
    print("--- Curl localhost:8081 ---")
    _, stdout, _ = ssh.exec_command('curl -I http://localhost:8081')
    print(stdout.read().decode())
    
    ssh.close()

if __name__ == "__main__":
    debug_502()
