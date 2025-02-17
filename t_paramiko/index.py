import paramiko


ssh = paramiko.SSHClient()
ssh.load_system_host_keys()
# ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
ssh.connect(hostname='192.168.100.95', username='inno', password='inno#2023')
stdin, stdout, stderr = ssh.exec_command('ls -la')
# print(f'{stdin=}')
# while stdout.seek():
#     print(stdout.readline())
stdout_output = stdout.read().decode()  # 读取输出
stderr_output = stderr.read().decode()  # 读取错误信息
print(stdout_output)
print(stderr_output)

# print(stderr.read())


# sftp_client = client.open_sftp()
# file_name_list = sftp_client.listdir('/home/inno')
# print(file_name_list)



if __name__ == '__main__':
    pass