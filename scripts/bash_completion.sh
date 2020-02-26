#!/usr/bin/env bash

_ramen_completions()
{
  COMPREPLY=($(compgen -W "$(${COMP_WORDS[0]} _completion " ${COMP_WORDS[*]:1}" | sed 's/\t/ /' | cut -d ' ' -f 1)"))
}

complete -F _ramen_completions ramen
