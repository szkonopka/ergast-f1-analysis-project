#!/bin/bash

sudo yum install gcc openssl-devel bzip2-devel libffi-devel

cd /usr/src
sudo wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz
sudo tar xzf Python-3.7.4.tgz

cd Python-3.7.4
sudo ./configure --enable-optimizations
sudo make altinstall

python3.7 --version
