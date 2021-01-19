#!/usr/bin/env bash

# Include colors.sh
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/colors.sh"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echoHeader "Deploying job definition to rundeck"

set +e

echo "GIT BRANCH: ${GIT_BRANCH}"
if [[ "${GIT_BRANCH}" == "master" ]]; then
    echo "POST to ${RUNDECK_ENDPOINT}"
    cat ${base_dir}/../../deploy/rundeck.yaml | curl -X POST 'http://'${RUNDECK_ENDPOINT}'/api/14/project/data_jobs/jobs/import?fileformat=yaml&dupeOption=update&uuidOption=preserve' -H 'Content-Type: application/yaml' -H 'X-Rundeck-Auth-Token: '${RUNDECK_TOKEN}'' --data-binary '@-'
else
    echo "POST to DEV"
    cat ${base_dir}/../../deploy/rundeck-dev.yaml | curl -X POST 'http://rundeck-bi01.dev.yapo.cl:4440/api/14/project/data_jobs/jobs/import?fileformat=yaml&dupeOption=update&uuidOption=preserve' -H 'Content-Type: application/yaml' -H 'X-Rundeck-Auth-Token: '${RUNDECK_TOKEN}'' --data-binary '@-'
fi
