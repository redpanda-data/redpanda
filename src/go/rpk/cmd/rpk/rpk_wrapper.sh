#!/bin/bash

rpk_path=$(readlink -f ${1})
shift

iotune_path=$(readlink -f ${1})
shift

cmd=${1}
shift

extra_cmd_args=""
if [ "${cmd}" = "iotune" ]; then
  extra_cmd_args="--iotune-path ${iotune_path}"
fi

root_dir=$BUILD_WORKSPACE_DIRECTORY
pushd "$root_dir"
${rpk_path} ${cmd} ${extra_cmd_args} "${@}"
popd
