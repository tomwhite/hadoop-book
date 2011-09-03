#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

$bin/check_manuscript.py  ~/workspace/htdg3/ch02.xml $actual/ch02/{MaxTemperature.xml,MaxTemperatureMapper.xml,MaxTemperatureReducer.xml,MaxTemperatureWithCombiner.xml}
