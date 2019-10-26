#!/bin/bash
set -e

if [ "$#" != 1 ]; then
  echo "Expecting one argument, the Slack webhook URL"
  exit 1
fi

this_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source $this_dir/../../../base_script.sh

log "deploying slack webhook to gcloud functions"

gcloud functions deploy subscribeSlack \
  --project redpandaci \
  --trigger-topic cloud-builds \
  --runtime nodejs10 \
  --set-env-vars "SLACK_WEBHOOK_URL=$1"
