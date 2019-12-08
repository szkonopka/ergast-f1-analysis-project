#!/bin/bash

sudo yum install gcc openssl-devel bzip2-devel libffi-devel

cd /usr/src
sudo wget https://www.python.org/ftp/python/3.6.2/Python-3.6.2.tgz
sudo tar xzf Python-3.6.2.tgz

cd Python-3.6.2
sudo ./configure --enable-optimizations --with-openssl=/usr/bin/openssl
sudo make altinstall

sudo python3.6 -m pip install --upgrade pip
python3.6 -m pip install --user hdfs
pyhton3.6 -m pip install --user pandas

python3.6 -m pip freeze
python3.6 --version
