#!/bin/bash
#
# Usage:
# hiver.sh
# hiver.sh -hiveconf fs.default.name=file:/// -hiveconf mapred.job.tracker=local

set -x
set -e

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

cd "$bin"/../..

#rm -rf $actual/ch12
mkdir -p $actual/ch12

for f in ch12/src/main/hive/*.hive; do
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
  python "$bin"/phragmite_hive.py $out $actual/ch12
  rm $out
done

