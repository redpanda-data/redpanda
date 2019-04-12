#!/bin/bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/base_script.sh"

git config format.signOff yes
git config diff.orderfile .gitorderfile

if [[ "$(git config user.name)" == "" ]]; then
    fatal "please set user name with 'git config user.name <user_name>'"
else 
    debug 'User name checked'
fi

if [[ "$(git config user.email)" != *"@vectorized.io" ]]; then
    fatal "Please set user email with 'git config user.email <user>@vectorized.io'"
else 
    debug "User email checked"
fi

log "Git config successfully verified"
