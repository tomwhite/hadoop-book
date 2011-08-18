cwd=$(pwd)
bindir=$(dirname $0)
rootdir=$bindir/..

function SETUP {
  cd $rootdir
  export HADOOP_HOME=~/dev/hadoop-0.20.2
  export PATH=$HADOOP_HOME/bin:$PATH
  export HADOOP_CONF_DIR=snippet/conf/pseudo
  hadoop fs -rmr input
  hadoop fs -put input .
  hadoop fs -rmr output-part-by-station
  rm -rf output
}

function TEARDOWN {
  cd $cwd
}

function TEST_MaxTemperatureByStationNameUsingDistributedCacheFile {
  source ch07/src/main/examples/PartitionByStationUsingMultipleOutputFormat.java.input.txt || return 1
  hadoop fs -getmerge output-part-by-station output || return 1
  #diff output snippet/expected/ch07/sh/part-00000 || return 1
}

source bashunit.sh