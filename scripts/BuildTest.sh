#!/bin/bash

#This script compiles the DataPreprocessor code and executes the unit and integration tests.

#Run Sbt Test
sbt test

#Check Sbt
TESTS_RESULT=`sbt test | grep "sbt.TestsFailedException" | wc -l`
zero=0;

if [ $TESTS_RESULT -ne $zero ]; then
    echo "[error] Deployment Terminated: $TESTS_RESULT test(s) failed!";
    exit 1;
fi

#Run Sbt Package
sbt package