cwd=$(pwd)
bindir=$(dirname $0)
rootdir=$bindir/..

function SETUP {
  cd $rootdir
  export HADOOP_HOME=~/dev/hadoop-0.20.2
  export PATH=$HADOOP_HOME/bin:$PATH
  export HADOOP_CONF_DIR=$bindir/conf/local
  export HADOOP_CLASSPATH=ch02/target/classes
  rm -rf output
}

function TEARDOWN {
  cd $cwd
}

function TEST_ch02 {
  source ch02/src/main/examples/MaxTemperature.java.input.txt || return 1
  diff output/part-00000 snippet/expected/ch02/sh/part-00000 || return 1
}

source bashunit.sh