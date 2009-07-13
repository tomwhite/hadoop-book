STREAM="hadoop jar $HADOOP_INSTALL/contrib/streaming/hadoop-*-streaming.jar -conf conf/hadoop-localhost.xml"

$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -input input/ncdc/all \
  -output out_max_daily \
  -mapper src/main/ch08/python/max_daily_temp_map.py \
  -reducer src/main/ch08/python/max_daily_temp_reduce.py \
  -file src/main/ch08/python/max_daily_temp_map.py \
  -file src/main/ch08/python/max_daily_temp_reduce.py

$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -input out_max_daily \
  -output out_mean_max_daily \
  -mapper src/main/ch08/python/mean_max_daily_temp_map.py \
  -reducer src/main/ch08/python/mean_max_daily_temp_reduce.py \
  -file src/main/ch08/python/mean_max_daily_temp_map.py \
  -file src/main/ch08/python/mean_max_daily_temp_reduce.py
  