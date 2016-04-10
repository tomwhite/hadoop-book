#!/bin/bash
set -x

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

perl $bin/phragmite_db.pl $actual/ch02/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch02-mr-intro)
perl $bin/phragmite_db.pl $actual/ch03/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch03-hdfs)
perl $bin/phragmite_db.pl $actual/ch05/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch05-io)
perl $bin/phragmite_db.pl $actual/ch12/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch12-avro)
perl $bin/phragmite_db.pl $actual/ch06/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch06-mr-dev)
perl $bin/phragmite_db.pl $actual/ch06/ $bin/../../../hadoop-book-mr-dev/pom.xml
perl $bin/phragmite_db.pl $actual/ch06/ $bin/../../ch06-mr-dev/src/main/resources/max-temp-workflow/workflow.xml
perl $bin/phragmite_db.pl $actual/ch08/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch08-mr-types)
perl $bin/phragmite_db.pl $actual/common/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../common)
perl $bin/phragmite_db.pl $actual/ch09/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch09-mr-features)
perl $bin/phragmite_db.pl $actual/ch21/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch21-zk)
