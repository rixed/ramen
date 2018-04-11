# vim:ft=sh

top_srcdir="."

fixtures="$top_srcdir/src/tests/fixtures"

nb_tests_tot=0
nb_tests_ok=0
ret=1

ramen_pids=""
do_at_exit=""
at_exit() {
  do_at_exit="$1; $do_at_exit"
}

# wc is such a pain
nb_lines() {
  sed -n '$=' $1
}

add_temp_file() {
  at_exit "rm -f '$1'"
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
export PROGRAM
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
  PROGRAM=""
  rm -f /tmp/ringbuf_*

  # OCAMLPATH=$top_srcdir/  useless with embedded compiler
  $ramen supervisor --no-demo -d --seed 1234 --use-embedded-compiler --bundle-dir=$top_srcdir/bundle &
  pid=$!
  add_temp_pid $pid
  if test -z "$ramen_pids" ; then
    ramen_pids="$pid"
  else
    ramen_pids="$ramen_pids,$pid"
  fi
}

upload() {
  file=$1
  func=$2
  curl -s -o /dev/null \
       --data-urlencode @"$top_srcdir/src/tests/fixtures/$file" \
       "$RAMEN_URL/upload/test/$func"
}

add_def() {
  PROGRAM=$PROGRAM"
$1
"
}

add_func() {
  # Beware that $2 might end with a comment
	add_def "DEFINE '$1' AS $2
;"
}

add_123() {
  add_func n123 "READ FILE \"$fixtures/123.csv\" (
    n u8 not null,  -- will be 1, 2, 3
    b bool not null,  -- true, true, false
    name string null) -- \"one\", \"two\" and NULL!"
}

nb_123=$(nb_lines $fixtures/123.csv)

add_cars() {
  add_func cars "READ FILE \"$fixtures/cars.csv\" (
    year u16 not null,
    manufacturer string not null,
    model string not null,
    horsepower u16 not null,
    CO float,
    CO2 float)"
}

nb_cars=$(nb_lines "$fixtures/cars.csv")

add_earthquakes() {
  add_func earthquakes "READ FILE \"$fixtures/earthquakes.csv\" SEPARATOR \"\\t\" (
    -- number of earthquakes per year
    year u16 not null,
    n u16 not null)"
}

nb_earthquakes=$(nb_lines "$fixtures/earthquakes.csv")

add_accounts() {
  add_func accounts "READ FILE \"$fixtures/accounts.csv\" (
    name string not null, amount float not null)"
}

nb_accounts=$(nb_lines "$fixtures/accounts.csv")

run() {
  eval "$ramen add --start --remote test '$PROGRAM'"
}

tail_() {
  # Also pass the total number of lines so that we can also wait for the end of the processing?
  total=$1
  wanted=$2
  func="test/$3"
  timeout --preserve-status 10s $ramen tail --last "$total" --as-csv $func |
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

  # Wait for ramen to actualy exit before deleting files, or it would
  # recreate those dirs to save its configuration one last time or even
  # would fail to kill some workers.
  if test -n "$ramen_pids" ; then
    # ps -p returns an error if none of the listed pids exists
    while ps -p $ramen_pids > /dev/null 2>&1 ; do
      echo "Waiting for ramen to exit..."
      sleep 0.4
    done
  fi

  if test "$ret" -eq 0 ; then
    eval "rm -rf $ALL_RAMEN_PERSIST_DIRS"
    rmdir /tmp/ramen_tests 2>/dev/null || true
  fi

  exit $ret
}

trap stop EXIT
trap stop 2
