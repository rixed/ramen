# vim:ft=sh

fixtures="$top_srcdir/tests/fixtures"

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

rigatoni="$top_srcdir/rigatoni"

export RAMEN_HTTP_PORT
export RAMEN_URL

start() {
  # RANDOM returns a number between 0 and 32767
  RAMEN_HTTP_PORT=$(shuf -i 1024-65536 -n 1)
  RAMEN_URL="http://127.0.0.1:$RAMEN_HTTP_PORT"
  rm -f /tmp/ringbuf_*

  $rigatoni start &
  add_temp_pid $!
  sleep 0.5
}

add_node() {
  $rigatoni add-node "$1" "$2"
  while test -n "$3" ; do
    $rigatoni add-link "$3" "$1"
    shift
  done
}

add_123() {
  add_node 123 "READ CSV FILE \"$fixtures/123.csv\" (n u8 not null, b bool not null)"
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

nb_cars=$(wc -l "$fixtures/cars.csv" | cut -d' ' -f 1)

run() {
  $rigatoni compile &&
  $rigatoni run
}

tail_() {
  $rigatoni tail --cont --last "$1" --as-csv "$2"
}

nb_tests_tot=0
nb_tests_ok=0
ret=0
check_equal() {
  nb_tests_tot=$((nb_tests_tot+1))
  if test "$1" = "$2" ; then
    nb_tests_ok=$((nb_tests_ok+1))
  else
    echo "Not equals: expected '$1' but got '$2'"
    ret=1
  fi
}

reset() {
  eval "$do_at_exit"
  do_at_exit=""
}

stop() {
  reset
  echo "$nb_tests_ok/$nb_tests_tot successful"
  exit $ret
}

trap stop EXIT
trap stop 2
