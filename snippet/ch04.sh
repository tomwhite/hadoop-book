cwd=$(pwd)
bindir=$(dirname $0)
rootdir=$bindir/..

function SETUP {
  cd $rootdir
  export HADOOP_HOME=~/dev/hadoop-0.20.2
  export PATH=$HADOOP_HOME/bin:$PATH
  export HADOOP_CONF_DIR=$bindir/conf/local
  export HADOOP_CLASSPATH=ch02/target/classes:ch04/target/classes
  rm -rf output
}

function TEARDOWN {
  cd $cwd
}

function TEST_MaxTemperatureWithCompression {
  source ch04/src/main/examples/MaxTemperatureWithCompression.java.input.txt || return 1
  gunzip -c output/part-00000.gz | diff - snippet/expected/ch02/sh/part-00000 || return 1
}

# TODO: debug why this fails
function IGNORE_TEST_MaxTemperatureWithMapOutputCompression {
  source ch04/src/main/examples/MaxTemperatureWithMapOutputCompression.java.input.txt || return 1
  diff output/part-00000 snippet/expected/ch02/sh/part-00000 || return 1
}

function TEST_NewMaxTemperatureWithCompression {
  source ch04/src/main/examples/NewMaxTemperatureWithCompression.java.input.txt || return 1
  gunzip -c output/part-r-00000.gz | diff - snippet/expected/ch02/sh/part-00000 || return 1
}

function TEST_NewMaxTemperatureWithMapOutputCompression {
  source ch04/src/main/examples/NewMaxTemperatureWithMapOutputCompression.java.input.txt || return 1
  diff output/part-r-00000 snippet/expected/ch02/sh/part-00000 || return 1
}

source bashunit.sh