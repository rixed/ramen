# vim:ft=sh

fixtures="$top_srcdir/src/tests/fixtures"

nb_tests_tot=0
nb_tests_ok=0
ret=1

do_at_exit=""
at_exit() {
  do_at_exit="$1; $do_at_exit"
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

start() {
  # RANDOM returns a number between 0 and 32767
  RAMEN_HTTP_PORT=$(shuf -i 1024-65536 -n 1)
  RAMEN_PERSIST_DIR="/tmp/ramen_tests/$RAMEN_HTTP_PORT"
  ALL_RAMEN_PERSIST_DIRS="$ALL_RAMEN_PERSIST_DIRS '$RAMEN_PERSIST_DIR'"
  RAMEN_URL="http://127.0.0.1:$RAMEN_HTTP_PORT"
  LAYER_CMD=""
  rm -f /tmp/ringbuf_*

  /usr/bin/env \
    OCAMLFIND_IGNORE_DUPS_IN="$(ocamlfind query ramen 2>/dev/null)" \
    $ramen start --no-demo -d &
  add_temp_pid $!
  sleep 1.5
}

add_node() {
  LAYER_CMD="$LAYER_CMD --op '$1:$2'"
  while test -n "$3" ; do
    LAYER_CMD="$LAYER_CMD"
    shift
  done
}

add_123() {
  add_node 123 "READ CSV FILE \"$fixtures/123.csv\" (
    n u8 not null,  -- will be 1, 2, 3
    b bool not null,  -- true, true, false
    name string null) -- \"one\", \"two\" and NULL!"
}

add_cars() {
  add_node cars "READ CSV FILE \"$fixtures/cars.csv\" (
    year u16 not null,
    manufacturer string not null,
    model string not null,
    horsepower u16 not null,
    CO float,
    CO2 float)"
}

nb_cars=$(wc -l "$fixtures/cars.csv" | awk '{print $1}')

add_earthquakes() {
  add_node earthquakes "READ CSV FILE \"$fixtures/earthquakes.csv\" SEPARATOR \"\\t\" (
    -- number of earthquakes per year
    year u16 not null,
    n u8 not null)"
}

nb_earthquakes=$(wc -l "$fixtures/earthquakes.csv" | awk '{print $1}')

add_accounts() {
  add_node accounts "READ CSV FILE \"$fixtures/accounts.csv\" (
    name string not null, amount float not null)"
}

nb_accounts=$(wc -l "$fixtures/accounts.csv" | awk '{print $1}')

run() {
  eval "$ramen add test $LAYER_CMD" &&
  $ramen compile &&
  $ramen run
  # We must give it time to process the CSV :(
  # FIXME: add the wait parameter to the tail cli command and use that instead.
  sleep 2
}

tail_() {
  $ramen tail --cont --last "$1" --as-csv "test/$2"
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
