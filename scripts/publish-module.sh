#!/bin/bash

MODULE_COMPILE=""

function GET_BUILD_MODULE(){
    GIT_LAST_COMMIT=$(git log -p --name-only --oneline | head -1 | awk '{print $1}')
    GIT_LAST_MERGE=$(git log -p --name-only --oneline | grep "Merge pull request #" | head -1 | awk '{print $1}')
    if [ "${GIT_LAST_COMMIT}" == "${GIT_LAST_MERGE}" ];
    then
        echo "Compare between Merge pull request"
        GIT_LAST_MERGE=$(git log -p --name-only --oneline | grep "Merge pull request #" | head -2 | tail -1 | awk '{print $1}')
    fi
    echo "GIT_CURRENT_BRANCH: ${BUILD_BRANCH}"
    echo "GIT_LAST_COMMIT: ${GIT_LAST_COMMIT}"
    echo "GIT_LAST_MERGE: ${GIT_LAST_MERGE}"
    MODULE_COMPILE=$(git log -p --name-only --oneline ${GIT_LAST_MERGE}..${GIT_LAST_COMMIT} | grep "/" | grep  -v " " | grep -v ".md" | awk '{split($0, val, "/"); print val[1]}' | sort | uniq -c | awk '{print $2}')
}

function PUBLISH_MODULE(){
    if [ -z "${MODULE_COMPILE}" ] || [ "${MODULE_COMPILE}" == "scripts" ];
    then
        echo "No changes detected."
    else
        COUNT_MODULES=$(echo "${MODULE_COMPILE}" | wc -l)
        let INCREMENT=1
        while [ ${INCREMENT} -le ${COUNT_MODULES} ];
        do
            MODULE=$(echo "${MODULE_COMPILE}" | head -${INCREMENT} | tail -1)
            if [ "${MODULE}" != "scripts" ]; then
                echo "make -C ${MODULE} docker-publish"
                make -C ${MODULE} docker-publish
                echo "make -C ${MODULE} rundeck-deploy"
                make -C ${MODULE} rundeck-deploy
            fi
            let INCREMENT=${INCREMENT}+1
        done
    fi
}

GET_BUILD_MODULE
PUBLISH_MODULE

