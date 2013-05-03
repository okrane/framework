from datetime import datetime
import paramiko

if __name__ == '__main__':
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('172.25.152.73', username='flexsys', password='flexsys1')
    
    cmd = 'ls ./logs/trades'
    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
    
    for line in stdout_grep:
        print line
        cmd2 = "ls ./logs/trades/%s | egrep '*.fix'" %line
        (stdin2, stdout_grep2, stderr2) = ssh.exec_command(cmd2)
        for line2 in stdout_grep2:
            print line2

#     date_str = '20130426-07:52:23'
#     
#     date = datetime.strptime(date_str, '%Y%m%d-%H:%M:%S')
#     
#     print date.minute