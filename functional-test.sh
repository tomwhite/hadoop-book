WORKSPACE=$(pwd)
TARBALL_URL=http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.6.0-cdh5.13.0.tar.gz
HADOOP_VERSION=2.6.0-cdh5.13.0

curl $TARBALL_URL | tar zxf -
export HADOOP_HOME=$WORKSPACE/$(basename $TARBALL_URL .tar.gz)
mvn clean verify -Dhadoop.version=$HADOOP_VERSION -DargLine="-Dhadoop.book.basedir=$WORKSPACE"