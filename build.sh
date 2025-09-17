#!/bin/bash
#

set -o errexit
catch() {
    echo 'catching!'
    if [ "$1" != "0" ]; then
    # error handling goes here
    echo "Error $1 occurred on $2"
    fi
}
trap 'catch $? $LINENO' EXIT

dbt_init_version="v0.1.0"

docker buildx build . \
  --pull \
  --tag 4fastbi/data-orchestrator-core:${dbt_init_version} \
  --platform linux/amd64 \
  --push
