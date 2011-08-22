cwd=$(pwd)
bindir=$(dirname $0)
rootdir=$bindir/..

function SETUP {
  cd $rootdir
  export HADOOP_HOME=~/dev/hadoop-0.20.2-cdh3u1
  export PATH=$HADOOP_HOME/bin:$PATH
  export HADOOP_CONF_DIR=$bindir/conf/local
  export HADOOP_CLASSPATH=common/target/classes:ch02/target/classes:ch04/target/classes:ch08/target/classes
  rm -rf output output-part-by-station
}

function TEARDOWN {
  cd $cwd
}

function TEST_NewJoinRecordWithStationName {
  source ch08/src/main/examples/NewJoinRecordWithStationName.java.input.txt || return 1
  # TODO: check output
}

function TEST_NewMaxTemperatureUsingSecondarySort {
  source ch08/src/main/examples/NewMaxTemperatureUsingSecondarySort.java.input.txt || return 1
  # TODO: check output
}

function TEST_NewMaxTemperatureWithCounters {
  source ch08/src/main/examples/NewMaxTemperatureWithCounters.java.input.txt || return 1
  # TODO: check output
}

source bashunit.sh