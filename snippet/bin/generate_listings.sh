#!/bin/bash
set -x

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual

perl $bin/phragmite_db.pl $actual/ch02-mr-intro/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch02-mr-intro)
perl $bin/phragmite_db.pl $actual/ch03-hdfs/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch03-hdfs)
perl $bin/phragmite_db.pl $actual/ch05-io/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch05-io)
perl $bin/phragmite_db.pl $actual/ch06-avro/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch06-avro)
perl $bin/phragmite_db.pl $actual/ch08-mr-dev/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch08-mr-dev)
perl $bin/phragmite_db.pl $actual/ch08-mr-dev/ $bin/../../../hadoop-book-mr-dev/pom.xml
perl $bin/phragmite_db.pl $actual/ch08-mr-dev/ $bin/../../ch08-mr-dev/src/main/resources/max-temp-workflow/workflow.xml
perl $bin/phragmite_db.pl $actual/ch10-mr-types/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch10-mr-types)
perl $bin/phragmite_db.pl $actual/common/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../common)
perl $bin/phragmite_db.pl $actual/ch11-mr-features/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch11-mr-features)
perl $bin/phragmite_db.pl $actual/ch21-zk/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch21-zk)
