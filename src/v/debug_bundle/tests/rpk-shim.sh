#!/usr/bin/env bash
set -euo pipefail

original_args=("$@")

output_file=""
dry_run=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --output)
      output_file="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

sleep_time="${RPK_SHIM_SLEEP_TIME:-5}"
exit_code="${RPK_SHIM_EXIT_CODE:-0}"

# In dry-run mode, emit JSON on stdout and skip writing any file; the real
# `rpk debug bundle --dry-run` emits a JSON probe report and creates no
# bundle artifact.
if [[ $dry_run -eq 1 ]]; then
  echo '{"probes":[{"category":"file","resource":"/proc/cpuinfo","ok":true}]}'
else
  if [[ -z $output_file ]]; then
    output_file=results.txt
  fi
  echo "${original_args[*]}" | tee "${output_file}"
fi

sleep "${sleep_time}"
exit "${exit_code}"
