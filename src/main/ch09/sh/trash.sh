hadoop fs -touchz quangle
hadoop fs -rm quangle
hadoop fs -lsr .Trash
hadoop fs -mv .Trash/Current/quangle .
hadoop fs -ls .