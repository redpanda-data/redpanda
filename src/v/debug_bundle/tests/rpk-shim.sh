#!/usr/bin/env bash
set -euo pipefail

original_args=("$@")

output_file=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --output)
      output_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [[ -z $output_file ]]; then
  output_file=results.txt
fi

sleep_time="${RPK_SHIM_SLEEP_TIME:-5}"

echo "${original_args[*]}" | tee "${output_file}"

sleep "${sleep_time}"
