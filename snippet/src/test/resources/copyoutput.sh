if [ ! -e output ]; then
  hadoop fs -get output .
fi
