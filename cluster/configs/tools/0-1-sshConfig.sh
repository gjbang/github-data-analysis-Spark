
#!/bin/bash

# ======== Attention ========
# This script needs to be executed 
# after all nodes have executed init.sh
# which means all nodes have a password to login

initPassWd=$1


# help to config ssh, main function:
# 1. generate ssh key
# 2. config local ssh authorized_keys
# 3. config sshd_config: all key/password login, 
# 4. create root password
echo "start 0-1 ssh config"
sudo apt -y update >/dev/null 2>&1
sudo apt -y install sshpass >/dev/null 2>&1

# generate ssh key
log_info "ssh config"
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# config ssh
echo -e "Port 22\nPubkeyAuthentication yes\n" >> /etc/ssh/sshd_config 
find /etc/ssh/ -name sshd_config | xargs perl -pi -e "s|PermitRootLogin no|PermitRootLogin yes|g"
# open password login, for ssh-copy id
find /etc/ssh/ -name sshd_config | xargs perl -pi -e "s|PasswordAuthentication no|PasswordAuthentication yes|g"
# not input yes when ssh-copy-id
sed -i '/StrictHostKeyChecking/c StrictHostKeyChecking no' /etc/ssh/ssh_config

# set a password for root, for ssh-copy-id
echo -e "root:$initPassWd" | chpasswd

# update config
service ssh restart