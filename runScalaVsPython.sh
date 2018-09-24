#!/bin/bash


echo "Testing Python"
./swapToLivyPython.sh
export CODE_KIND="pyspark"
timeout 120s ./perfTestLivy.py -o LivyPythonResults.log

echo "Testing Scala"
./swapToLivy.sh
export CODE_KIND="spark"
timeout 120s ./perfTestLivy.py -o LivyScalaResults.log
