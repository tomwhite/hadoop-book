#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual
book_workspace=~/book-workspace/htdg-git

# Should add remaining chapters: ch16-pig ch17-hive ch21-zk
for ch in ch02-mr-intro ch03-hdfs ch05-io ch08-mr-dev ch10-mr-types ch11-mr-features
do
  # remove id attributes from program listings, and add a newline before </programlisting>
  sed '/<programlisting/s/ id="[^"]*"//; s|</programlisting>|\
</programlisting>|' $book_workspace/$ch.xml > /tmp/$ch.xml
  $bin/check_manuscript.py /tmp/$ch.xml $actual/$ch/*
done

# Avro check
sed -e '/<programlisting/s/ id="[^"]*"//; s|</programlisting>|\
</programlisting>|' $book_workspace/ch06-avro.xml > /tmp/ch06-avro.xml
$bin/check_manuscript.py /tmp/ch06-avro.xml $actual/ch06-avro/*

# Common check
$bin/check_manuscript.py /tmp/ch10-mr-types.xml $actual/common/*
