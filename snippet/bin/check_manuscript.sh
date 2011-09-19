#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

$bin/check_manuscript.py  ~/workspace/htdg3/ch02.xml $actual/ch02/*
$bin/check_manuscript.py  ~/workspace/htdg3/ch04.xml $actual/ch04/*
$bin/check_manuscript.py  ~/workspace/htdg3/ch05.xml $actual/ch05/*