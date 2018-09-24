#!/bin/bash

CMD=timeout
if [[ $OSTYPE == darwin* ]]; then
  CMD=gtimeout
fi

echo "Testing Python"
./swapToLivyPython.sh
$CMD 120s ./perfTestLivy.py -k pyspark -o LivyPythonResults.log

echo "Testing Scala"
./swapToLivy.sh
$CMD 120s ./perfTestLivy.py -k spark -o LivyScalaResults.log
