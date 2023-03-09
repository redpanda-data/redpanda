#!/bin/bash

while getopts 'r:b:c:-:' OPTION; do
  case "${OPTION}" in
    r)
      PANDAPROXY_RETRIES="${OPTARG}"
      ;;
    b)
      BRANCH="${OPTARG}"
      ;;
    c)
      CRASH_LOOP_LIMIT="${OPTARG}"
      ;;
    -)
      # kuttl adds a "--namespace" flag
      ;;
    ?)
      echo "usage: $(basename $0) -r <pandaproxy retries> [-c <crash loop limit>] [-b <branch>]"
      exit 1
      ;;
  esac
done

if [ -z "$PANDAPROXY_RETRIES" ]; then
  echo "requires one argument, pandaproxy retries count"
  exit 1
fi

BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD | sed 's/\.x%//')}
if ! (echo "${BRANCH}" | grep -q -E 'v2[2-9]\.[1-4]|dev'); then
  echo "BRANCH must be in the form of 'v2[2-9]\.[1-4]|dev' (ie 'v23.1'), got ${BRANCH}"
  echo "Using 'dev'"
  BRANCH=dev
fi

if [[ ${BRANCH} == "dev" && -z $CRASH_LOOP_LIMIT ]]; then
  echo "requires two argument, pandaproxy retries count and crash loop limit"
  exit 1
fi

echo "Verifying config for ${BRANCH} with args: $*"

# shellcheck source=verify-config-dev.sh
source "verify-config-${BRANCH}.sh"

retries=20
until [ "$retries" -lt 0 ]; do
  echo "Fetching configuration from $NAMESPACE/additional-configuration-0"
  actual=$(kubectl -n "$NAMESPACE" exec additional-configuration-0 -- cat /etc/redpanda/redpanda.yaml)

  echo Actual config:
  echo "$actual"
  echo
  echo Difference:
  diff -b <(echo "$actual") <(echo "$expected") && exit 0
  echo "Retrying... ({$retries} left)"
  sleep 5
  ((retries = retries - 1))
done
echo "ERROR: out of retries"
exit 1
