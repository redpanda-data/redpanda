#!/bin/bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/base_script.sh"

git config format.signOff yes

if [[ "$(git config user.name)" == "" ]]; then
    fatal "please set user name with 'git config user.name <user_name>'"
else 
    debug 'User name checked'
fi

if [[ "$(git config user.email)" == "" ]]; then
    fatal "Please set user email with 'git config user.email <email>'"
else 
    debug "User email checked"
fi

log "Git config successfully verified"
