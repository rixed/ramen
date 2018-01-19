# vim:ft=sh

fixtures="$top_srcdir/src/tests/fixtures"

nb_tests_tot=0
nb_tests_ok=0
ret=1

do_at_exit=""
at_exit() {
  do_at_exit="$1; $do_at_exit"
}

# wc is such a pain
nb_lines() {
  sed -n '$=' $1
}

add_temp_file() {
  at_exit "rm -f '$temp_files'"
}

kill_recurs() {
  local pid="$1"
  pgrep -P "$pid" | while read child ; do
    kill_recurs "$child" ;
  done
  kill "$pid" || true # may have died already
}

add_temp_pid() {
  at_exit "kill_recurs '$1'"
}

file_with() {
  f=$(tempfile)
  add_temp_file "$f"
  cat > "$f"
}

ramen="$top_srcdir/src/ramen"

export RAMEN_HTTP_PORT
export RAMEN_PERSIST_DIR
export RAMEN_URL
export LAYER_CMD
# We just know if the whole test suites have been successful or not so we have
# to either delete all temp dirs or none. Note that it is OK to `rm -rf ''`.
export ALL_RAMEN_PERSIST_DIRS
export OCAMLFIND_IGNORE_DUPS_IN
OCAMLFIND_IGNORE_DUPS_IN="$(ocamlfind query ramen 2>/dev/null || true)"
export OCAMLRUNPARAM
OCAMLRUNPARAM=b

start() {
  # RANDOM returns a number between 0 and 32767
  RAMEN_HTTP_PORT=$(shuf -i 1024-65536 -n 1)
  RAMEN_PERSIST_DIR="/tmp/ramen_tests/$RAMEN_HTTP_PORT"
  ALL_RAMEN_PERSIST_DIRS="$ALL_RAMEN_PERSIST_DIRS '$RAMEN_PERSIST_DIR'"
  RAMEN_URL="http://127.0.0.1:$RAMEN_HTTP_PORT"
  LAYER_CMD=""
  rm -f /tmp/ringbuf_*

  OCAMLPATH=$top_srcdir $ramen start --no-demo -d --seed 1234 &
  add_temp_pid $!
}

upload() {
  file=$1
  node=$2
  curl -s -o /dev/null \
       --data-urlencode @"$top_srcdir/src/tests/fixtures/$file" \
       "$RAMEN_URL/upload/test/$node"
}

add_node() {
  LAYER_CMD="$LAYER_CMD --op '$1:$2'"
}

add_123() {
  add_node 123 "READ FILE \"$fixtures/123.csv\" (
    n u8 not null,  -- will be 1, 2, 3
    b bool not null,  -- true, true, false
    name string null) -- \"one\", \"two\" and NULL!"
}

nb_123=$(nb_lines $fixtures/123.csv)

add_cars() {
  add_node cars "READ FILE \"$fixtures/cars.csv\" (
    year u16 not null,
    manufacturer string not null,
    model string not null,
    horsepower u16 not null,
    CO float,
    CO2 float)"
}

nb_cars=$(nb_lines "$fixtures/cars.csv")

add_earthquakes() {
  add_node earthquakes "READ FILE \"$fixtures/earthquakes.csv\" SEPARATOR \"\\t\" (
    -- number of earthquakes per year
    year u16 not null,
    n u16 not null)"
}

nb_earthquakes=$(nb_lines "$fixtures/earthquakes.csv")

add_accounts() {
  add_node accounts "READ FILE \"$fixtures/accounts.csv\" (
    name string not null, amount float not null)"
}

nb_accounts=$(nb_lines "$fixtures/accounts.csv")

run() {
  eval "$ramen add test $LAYER_CMD" &&
  $ramen compile &&
  $ramen run
}

tail_() {
  # Also pass the total number of lines so that we can also wait for the end of the processing?
  total=$1
  wanted=$2
  node="test/$3"
  timeout --preserve-status 10s $ramen tail --last "$total" --as-csv $node |
    tail -n "$wanted"
}

check_equal() {
  nb_tests_tot=$((nb_tests_tot+1))
  if test "$1" = "$2" ; then
    nb_tests_ok=$((nb_tests_ok+1))
  else
    echo
    echo "Not equals: expected '$1' but got '$2'"
    echo
  fi
}

reset() {
  eval "$do_at_exit"
  do_at_exit=""
}

stop() {
  reset
  echo "$nb_tests_ok/$nb_tests_tot successful"
  if test -n "$expected_tests" && test "$expected_tests" -lt "$nb_tests_tot" ; then
    echo "More tests run that expected, isn't that weird?"
  elif test -n "$expected_tests" && ! test "$expected_tests" -eq "$nb_tests_tot" ; then
    echo "$((expected_tests-nb_tests_tot)) not run!"
  elif test "$nb_tests_ok" -eq "$nb_tests_tot" ; then
    echo SUCCESS
    ret=0
  fi

  if test "$ret" -eq 0 ; then
    eval "rm -rf $ALL_RAMEN_PERSIST_DIRS"
  fi

  exit $ret
}

trap stop EXIT
trap stop 2
