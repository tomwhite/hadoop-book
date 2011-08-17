set -e

SCRIPT_NAME=$0

function get_tests() {
  part1=function
  part2=TEST_
  grep "$part1 $part2" $SCRIPT_NAME | sed 's/.*\(TEST_[A-Za-z0-9_]*\).*/\1/'
}

function run_tests() {
  local exit_code=0

  for t in $(get_tests); do
    echo "RUNNING: $t"
    SETUP
    if "$t"; then
      TEARDOWN
      echo "PASSED: $t"
    else
      TEARDOWN
      echo "FAILED: $t"
      return 1
    fi
  done
}

if run_tests; then
  echo "ALL TESTS PASSED"
  exit 0
else
  echo "A TEST FAILED"
  exit 1
fi
