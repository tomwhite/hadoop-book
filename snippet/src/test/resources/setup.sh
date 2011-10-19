if ! hadoop fs -test -e input; then
  hadoop fs -put input .
fi
if hadoop fs -test -e output; then
  hadoop fs -rmr output
fi
if [ -e output ]; then
  rm -r output
fi
