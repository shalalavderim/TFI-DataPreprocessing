#!/bin/bash

#This script establishes the environment to compile, run tests and deploy the SKParser to Azure.

#Install Java
sudo apt-get update
sudo apt-get install -y openjdk-8-jdk

#Install Scala
sudo apt-get install -y wget
wget www.scala-lang.org/files/archive/scala-2.12.11.deb
dpkg -i scala*.deb

#Install Sbt
echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-get install -y gnupg2
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt -y

#Install Python Libraries
sudo apt-get install python3-setuptools
pip3 install wheel

#Install databricks-cli
pip3 install databricks-cli
export DATABRICKS_HOST=https://adb-1952563685381701.1.azuredatabricks.net/
export DATABRICKS_TOKEN=$conntoken
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

#Test databricks CLI
sudo chmod 777 scripts/databricksrun.sh
./scripts/databricksrun.sh --version