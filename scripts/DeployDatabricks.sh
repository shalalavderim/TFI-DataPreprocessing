#!/bin/bash

#This script deploys the succesfully tested and compiled DataPreprocessor to Databricks Jobs on Azure.

#Configure databricks CLI
export DATABRICKS_HOST=https://adb-1952563685381701.1.azuredatabricks.net/
export DATABRICKS_TOKEN=$conntoken
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

#Copy Jar and configs to databricks tmp
./scripts/databricksrun.sh fs rm -r dbfs:/tmp/jars/tfi-datapreprocessor
./scripts/databricksrun.sh fs cp --overwrite target/scala-*/tfi-datapreprocessor*.jar dbfs:/tmp/jars/tfi-datapreprocessor/tfi-datapreprocessor.jar
./scripts/databricksrun.sh fs cp --overwrite conf/TFI-DataPreprocessor.conf dbfs:/mnt/raw-data/ConfigFiles/TFI-DataPreprocessor.conf

#Update existing job or create new job for SKParser in Q
DataPreprocessorName="job-preprocessor-tfi"
DataPreprocessorId=$(./scripts/databricksrun.sh jobs list | grep "$DataPreprocessorName" | cut -d " " -f1)
DataPreprocessorSize=${#DataPreprocessorId}
if [ $DataPreprocessorSize -ge 1 ]
then
      ./scripts/databricksrun.sh jobs reset --job-id $DataPreprocessorId --json-file scripts/DataPreprocessor.json
else
      ./scripts/databricksrun.sh jobs create --json-file scripts/DataPreprocessor.json
fi
echo "Successfully deployed new version of TFI-DataPreprocessor"