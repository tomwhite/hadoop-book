#!/bin/bash

set -x
set -e

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

cd "$bin"/../..

rm -rf $actual/ch16
mkdir -p $actual/ch16

for f in ch16-pig/src/main/grunt/*.grunt; do
  out=$f.output.txt
  pig -x local < $f 2> /dev/null \
      | grep -v INFO \
      | grep -v '^grunt> $' \
      | sed -e 's|&|\&amp;|g' \
            -e 's|"|\&quot;|g' \
            -e 's|>|\&gt;|g' \
            -e 's|<|\&lt;|g' \
            -e 's|^\(grunt&gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
            -e 's|^\(&gt;&gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
      > $out
  cat $out
  python "$bin"/phragmite_pig.py $out $actual/ch16
  rm $out
done

