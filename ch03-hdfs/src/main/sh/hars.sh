: == har_ls_files
: == har_create
: == har_inspect
: == har_ls
: == har_ls_long
: == har_rmr
rsync -avz --exclude '.svn' /Users/tom/workspace/htdg/input/fileinput/ /tmp/fileinput
hadoop fs -copyFromLocal /tmp/fileinput /my/files
rm -rf /tmp/fileinput
: vv har_ls_files
hadoop fs -lsr /my/files
: ^^ har_ls_files
: vv har_create
hadoop archive -archiveName files.har /my/files /my
: ^^ har_create
: vv har_inspect
hadoop fs -ls /my
hadoop fs -ls /my/files.har
: ^^ har_inspect
: vv har_ls
hadoop fs -lsr har:///my/files.har
: ^^ har_ls
: vv har_ls_long
hadoop fs -lsr har:///my/files.har/my/files/dir
hadoop fs -lsr har://hdfs-localhost:8020/my/files.har/my/files/dir
: ^^ har_ls_long
: vv har_rmr
hadoop fs -rmr /my/files.har
: ^^ har_rmr
hadoop fs -rmr /my/files