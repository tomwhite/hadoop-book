: == group_create
: == group_list_empty
: == group_join
: == group_list_after_join
: == group_kill_goat
: == group_list_after_kill
: == group_delete
: vv group_create
java CreateGroup localhost zoo
: ^^ group_create
: vv group_list_empty
java ListGroup localhost zoo
: ^^ group_list_empty
: vv group_join
java JoinGroup localhost zoo duck &
duck_pid=$!
java JoinGroup localhost zoo cow &
cow_pid=$!
java JoinGroup localhost zoo goat &
goat_pid=$!
: ^^ group_join
sleep 5
: vv group_list_after_join
java ListGroup localhost zoo
: ^^ group_list_after_join
: vv group_kill_goat
kill $goat_pid
: ^^ group_kill_goat
sleep 5
sleep 5 # be sure goat process has died
: vv group_list_after_kill
java ListGroup localhost zoo
: ^^ group_list_after_kill
kill $duck_pid
kill $cow_pid
: vv group_delete
java DeleteGroup localhost zoo
java ListGroup localhost zoo
: ^^ group_delete