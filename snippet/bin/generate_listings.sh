#!/bin/bash
set -x

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

perl $bin/phragmite_db.pl $actual/ch02/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch02)
perl $bin/phragmite_db.pl $actual/ch04/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch04)
perl $bin/phragmite_db.pl $actual/ch05/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch05)
