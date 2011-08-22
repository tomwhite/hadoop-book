cwd=$(pwd)
bindir=$(dirname $0)
rootdir=$bindir/..

function SETUP {
  cd $rootdir
  export HADOOP_HOME=~/dev/hadoop-0.20.2-cdh3u1
  export PATH=$HADOOP_HOME/bin:$PATH
  export HADOOP_CONF_DIR=$bindir/conf/local
  export HADOOP_CLASSPATH=common/target/classes:ch07/target/classes
  rm -rf output output-part-by-station
}

function TEARDOWN {
  cd $cwd
}

function TEST_NewPartitionByStationUsingMultipleOutputs {
  source ch07/src/main/examples/NewPartitionByStationUsingMultipleOutputs.input.txt || return 1
  # TODO: check output
}

function TEST_NewPartitionByStationYearUsingMultipleOutputs {
  source ch07/src/main/examples/NewPartitionByStationYearUsingMultipleOutputs.input.txt || return 1
  # TODO: check output
}

function TEST_minimal_map_reduce {
  source ch07/src/main/examples/MinimalMapReduce.input.txt || return 1
  diff output/part-00000 snippet/expected/ch07/sh/part-00000 || return 1
}

function TEST_minimal_map_reduce_with_defaults {
  source ch07/src/main/examples/MinimalMapReduceWithDefaults.input.txt || return 1
  diff output/part-00000 snippet/expected/ch07/sh/part-00000 || return 1
}

function TEST_new_minimal_map_reduce {
  source ch07/src/main/examples/NewMinimalMapReduce.input.txt || return 1
  diff output/part-r-00000 snippet/expected/ch07/sh/part-00000 || return 1
}

function TEST_new_minimal_map_reduce_with_defaults {
  source ch07/src/main/examples/NewMinimalMapReduceWithDefaults.input.txt || return 1
  diff output/part-r-00000 snippet/expected/ch07/sh/part-00000 || return 1
}

function TEST_MaxTemperatureByStationNameUsingDistributedCacheFile {
  source ch07/src/main/examples/PartitionByStationUsingMultipleOutputFormat.java.input.txt || return 1
}

source bashunit.sh