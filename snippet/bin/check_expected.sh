#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual
expected="$bin"/../expected

for f in $expected/ch11/grunt/*.xml; do
  echo $f
  f_actual=$actual/ch11/grunt/`basename $f`
  diff $f $f_actual > /dev/null
  if [ $? != 0 ]; then
    echo "Expected file $f different to actual $f_actual:"
    diff $f $f_actual
    #exit 1
  fi
done