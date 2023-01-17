#!/bin/bash
set -e

go_bin=${1}
output=${2}

(cd $(dirname ${output}) &&
  ${go_bin} build -modcacherw -o $(basename ${output}))
