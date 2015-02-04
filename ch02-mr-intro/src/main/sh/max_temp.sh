: == max_temp_java
: == max_temp_java_output
: == max_temp_ruby_map
: == max_temp_ruby_pipeline
: == max_temp_python_pipeline
rm -r /Users/tom/workspace/htdg/output
: vv max_temp_java
export HADOOP_CLASSPATH=build/classes
hadoop MaxTemperature input/ncdc/sample.txt output
: ^^ max_temp_java
: vv max_temp_java_output
cat output/part-00000
: ^^ max_temp_java_output
: vv max_temp_ruby_map
cat input/ncdc/sample.txt | ch02-mr-intro/src/main/ruby/max_temperature_map.rb
: ^^ max_temp_ruby_map
: vv max_temp_ruby_pipeline
cat input/ncdc/sample.txt | \
  ch02-mr-intro/src/main/ruby/max_temperature_map.rb | \
  sort | ch02-mr-intro/src/main/ruby/max_temperature_reduce.rb
: ^^ max_temp_ruby_pipeline
: vv max_temp_python_pipeline
cat input/ncdc/sample.txt | \
  ch02-mr-intro/src/main/python/max_temperature_map.py | \
  sort | ch02-mr-intro/src/main/python/max_temperature_reduce.py
: ^^ max_temp_python_pipeline
  