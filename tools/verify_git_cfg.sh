#!/bin/bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verified=true
source "${script_dir}/base_script.sh"

git config format.signOff yes
git config diff.orderfile .gitorderfile

if [[ "$(git config user.name)" == "" ]]; then
    error "please set user name with 'git config user.name <user_name>'"
    verified=false
fi

if [[ "$(git config user.email)" != *"@vectorized.io" ]]; then
    error "Please set user email with 'git config user.email <user>@vectorized.io'"
    verified=false
fi

if $verified; then
    log "Git config successfully verified"
fi
