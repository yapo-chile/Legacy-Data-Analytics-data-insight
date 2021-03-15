#!/bin/bash

# Include colors.sh
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/colors.sh"

set -e

echoHeader "Running Checkstyle Tests"

if [[ -n "$TRAVIS" ]]; then
    mkdir -p ${REPORT_ARTIFACTS}
    CHECKSTYLE_FILE=${REPORT_ARTIFACTS}/checkstyle-report.xml
    pylint -f json -r n app/ | ./scripts/commands/pylint-to-checkstyle > ${CHECKSTYLE_FILE}
else
    pylint -r n app/
fi
status=${PIPESTATUS[0]}

# We need to catch error codes that are bigger then 2,
# they signal that pylint exited because of underlying error.
if [ ${status} -ge 1 ]; then
    echo "pylint exited with code ${status}, check pylint errors"
    exit ${status}
fi
exit 0