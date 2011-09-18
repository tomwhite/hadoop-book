#!/bin/bash
set -x

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

perl $bin/phragmite_db.pl $actual/ch02/ $bin/../../ch02/src/main/java/*.java
perl $bin/phragmite_db.pl $actual/ch04/ $bin/../../ch04/src/main/java/*.java
perl $bin/phragmite_db.pl $actual/ch05/ $bin/../../ch05/src/main/java/*.java $bin/../../ch05/src/test/java/*.java
perl $bin/phragmite_db.pl $actual/ch05/ $bin/../../ch05/src/main/java/v?/*.java $bin/../../ch05/src/test/java/v?/*.java