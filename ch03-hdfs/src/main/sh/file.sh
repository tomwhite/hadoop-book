: == url_cat
: == filesystem_cat
: == filesystem_double_cat
: == list_status
: == file_copy_with_progress
rm -r /Users/tom/workspace/htdg/output
export HADOOP_CLASSPATH=build/classes
: vv url_cat
hadoop URLCat hdfs://localhost/user/tom/quangle.txt
: ^^ url_cat
: vv filesystem_cat
hadoop FileSystemCat hdfs://localhost/user/tom/quangle.txt
: ^^ filesystem_cat
: vv filesystem_double_cat
hadoop FileSystemDoubleCat hdfs://localhost/user/tom/quangle.txt
: ^^ filesystem_double_cat
: vv list_status
hadoop ListStatus hdfs://localhost/ hdfs://localhost/user/tom
: ^^ list_status
: vv file_copy_with_progress
hadoop FileCopyWithProgress input/docs/1400-8.txt hdfs://localhost/user/tom/1400-8.txt
: ^^ file_copy_with_progress


