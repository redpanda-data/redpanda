#!/bin/bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/base_script.sh"

if [[ "$(git config format.signOff)" != "yes" ]]; then
    fatal "Please set auto signOff in Git with 'git config --global format.signOff yes'"
else 
    log "Autmatic sign off checked"
fi

if [[ "$(git config user.name)" == "" ]]; then
    fatal "please set user name with 'git config --global user.name <user_name>'"
else 
    debug 'User name checked'
fi

if [[ "$(git config user.email)" == "" ]]; then
    fatal "Please set user email with 'git config --global user.email <email>'"
else 
    debug "User email checked"
fi

log "Git config successfully verified"