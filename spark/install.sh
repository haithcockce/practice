#!/bin/bash

sudo dnf install wget java tar vim pip postgresql postgresql-server -y
pip install ipython pyspark ipdb sklearn
sudo postgresql-setup --initdb --unit postgresql
systemctl enable postgresql
systemctl start postgresql
su - postgres -c "createdb iris"

sudo mkdir /usr/bin/spark
wget http://www.gtlib.gatech.edu/pub/apache/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
tar -xf spark-3.0.0-preview2-bin-hadoop2.7.tgz
sudo mv spark-3.0.0-preview2-bin-hadoop2.7.tgz/* /usr/bin/spark
mkdir ~/spark
cd ~/spark
wget https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
wget https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.names

