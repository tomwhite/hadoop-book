#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

# Should add remaining chapters: ch03 ch04 ch14
for ch in ch02 ch05 ch07 ch08 ch11 ch12
do
  # remove any id attributes from program listings
  sed '/<programlisting/s/ id=".*"//' ~/book-workspace/htdg3/$ch.xml > /tmp/$ch.xml
  $bin/check_manuscript.py  /tmp/$ch.xml $actual/$ch/*
done

# Avro check
sed '/<programlisting/s/ id=".*"//' ~/book-workspace/htdg3/ch04.xml > /tmp/ch04-avro.xml
$bin/check_manuscript.py  /tmp/ch04-avro.xml $actual/ch04-avro/*

# Common check
$bin/check_manuscript.py  ~/book-workspace/htdg3/ch07.xml $actual/common/*
