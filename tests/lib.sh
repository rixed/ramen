set -e

# At exit

_AT_EXIT='echo bye'
at_exit() {
  _AT_EXIT="$*; $_AT_EXIT"
}
at_exit_() {
  eval "$_AT_EXIT" || true
  _AT_EXIT='true'
}
trap at_exit_ EXIT INT TERM

# Logs

debug=yes

log() {
  if test "$debug" = yes; then
    echo "$*" 1>&2
  fi
}

fail() {
  echo "$*" 1>&2
  exit 1
}

# Locate the required tools

find_bin() {
  tool=$1; shift
  log "looking for '$tool' (in '$*')"

  while test -n "$*"; do
    p=$1; shift
    if test -x "$p/$tool"; then
      log "...found in $p"
      echo "$p/$tool"
      return
    fi
  done

  if test -n "$(which $tool)"; then
    which "$tool"
    return
  fi

  fail "Cannot find $tool"
}

ramen="$top_src/src/ramen"
export RAMEN_LIBS="$top_src/bundle"

# Locate the optional tools
# TODO: if there is atop, run it

# Prepare for a fresh RAMEN_DIR
export RAMEN_DIR=$(mktemp -d)
#at_exit "echo 'Cleaning ramen dir $RAMEN_DIR'; rm -rf '$RAMEN_DIR'"
