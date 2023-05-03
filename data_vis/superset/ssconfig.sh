#!/bin/bash

# This shell config apache superset on ubuntu 22.04 on aliyun ecs
sudo apt-get install build-essential libssl-dev libffi-dev python3-dev python3-pip libsasl2-dev libldap2-dev default-libmysqlclient-dev python3.10-venv libnss3-tools -y

pip install virtualenv

python3 -m venv venv
. venv/bin/activate

# Here we name the virtual env 'superset'
# pyenv virtualenv superset
# pyenv activate superset

pip install apache-superset
pip install sqlparse=='0.4.3'

export PYTHONPATH=/superset_config.py:$PYTHONPATH 
export FLASK_APP=superset 


superset fab create-admin
# spark
# spark
# 123456

superset db upgrade

superset init

pip install gunicorn -i https://pypi.douban.com/simple/
# pip install --force-reinstall MarkupSafe==2.0.1

gunicorn --workers 2 --timeout 120 --bind worker01:8787  "superset.app:create_app()" --daemon 