#!/usr/bin/env bash

_ramen_completions()
{
  if [ -x "$(command -v ${COMP_WORDS[0]})" ] ; then
    COMPREPLY=($(compgen -W "$(${COMP_WORDS[0]} _completion " ${COMP_WORDS[*]:1}" | sed 's/\t/ /' | cut -d ' ' -f 1)"))
  fi
}

complete -F _ramen_completions ramen
