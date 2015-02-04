#!/bin/bash
#
# Usage:
# hiver.sh
# hiver.sh -hiveconf fs.defaultFS=file:/// -hiveconf mapreduce.framework.name=local

set -x
set -e

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

cd "$bin"/../..

#rm -rf $actual/ch17
mkdir -p $actual/ch17

for f in ch17-hive/src/main/hive/*.hive; do
  out=$f.output.txt
  hive "$@" -S < $f 2> /dev/null \
    | grep -v '^hive> $' \
    | grep -v '^$' \
    | sed -e 's|&|\&amp;|g' \
          -e 's|"|\&quot;|g' \
          -e 's|>|\&gt;|g' \
          -e 's|<|\&lt;|g' \
          -e 's|	|    |g' \
          -e 's|^\(hive&gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
          -e 's|^\(    &gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
          -e 's|Hank    |Hank   |g' \
          -e 's|0    NULL    NULL|0    NULL NULL|' \
          -e 's|NULL    NULL    |NULL   NULL |' \
    > $out
   
  cat $out
  python "$bin"/phragmite_hive.py $out $actual/ch17
  rm $out
done

