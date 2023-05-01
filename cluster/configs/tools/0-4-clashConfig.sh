
#!/bin/bash

# ======== Attention ========

userName=$1
initPassWd=$2

# print format log information
log_info(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[34m [info] \033[m $1"
}

log_debug(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[32m [debug] \033[m $1"
}

log_warn(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[33m [warning] \033[m $1" 
}

log_error(){
    echo -e "`date +%m-%d-%H:%M:%S`:\033[31m [error] \033[m $1"
}

# get executable file -- clash
gunzip $HOME/configs/clash/clash-linux-amd64-v1.15.1.gz
mv clash-linux-amd64-v1.15.1 $HOME/configs/clash/clash
sudo chmod a+x $HOME/configs/clash/clash
cp $HOME/configs/clash/clash /usr/local/bin/clash

# cp config file
cp $HOME/configs/clash/config.yaml $HOME/.config/clash/config.yaml
cp $HOME/configs/clash/Country.mmdb $HOME/configs/clash/Country.mmdb

# config system service
cp $HOME/configs/clash/clash.service /etc/systemd/system/clash.service

# add agency
echo -e "export https_proxy=http://127.0.0.1:7890" >> ~/.bashrc
echo -e "export http_proxy=http://127.0.0.1:7890" >> ~/.bashrc
echo -e "export all_proxy=socks5://127.0.0.1:7891" >> ~/.bashrc

source ~/.bashrc

# set system service
systemctl daemon-reload
# to start clash service when system boot
systemctl enable clash
# start clash service right away
systemctl start clash

# verify if clash service can visit foreign website
curl -I https://www.google.com > $HOME/configs/logs/clash.log

log_info "clash config finished"