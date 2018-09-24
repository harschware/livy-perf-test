#!/bin/bash

perl -ne 'if( /(\d+)s/ ) {$c++; $a+=$1}; END {print $a/$c, "\n"}' $1
