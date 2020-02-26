#!/bin/bash

sudo dnf install tar vim wget pip git postgresql postgresql-server
pip install pyspark ipython sklearn ipdb
sudo postgresql-setup --initdb --unit postgresql
sudo systemctl enable postgresql-server
sudo systemctl start postgresql-server
mkdir ~/spark
cd ~/spark
wget http://apache.cs.utah.edu/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
wget https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
wget https://jdbc.postgresql.org/download/postgresql-42.2.10.jar
sudo mkdir /usr/bin/spark
sudo tar -xf spark-2.4.5-bin-hadoop2.7.tgz -C /usr/bin/spark
sudo mv /usr/bin/spark/*/* /usr/bin/spark
export PATH="$PATH:/usr/bin/spark"
