#!/usr/bin/env bash

# Include colors.sh
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/colors.sh"

#Build code again now for docker platform
echoHeader "Building code for docker platform"

set +e

echoTitle "Starting Docker Engine"
if [[ $OSTYPE == "darwin"* ]]; then
    echoTitle "Starting Mac OSX Docker Daemon"
    #$DIR/docker-start-macosx.sh
elif [[ "$OSTYPE" == "linux-gnu" ]]; then
    echoTitle "Starting Linux Docker Daemon"
    sudo start-docker-daemon
else
    echoError "Platform not supported"
fi

if [ -n "${BUILD_BRANCH}" ]; then
    export GIT_BRANCH=${BUILD_BRANCH}
fi

echoTitle "Building docker image for ${DOCKER_IMAGE}"
echo "GIT BRANCH: ${GIT_BRANCH}"
echo "GIT TAG: ${GIT_TAG}"
echo "GIT COMMIT: ${GIT_COMMIT}"
echo "GIT COMMIT SHORT: ${GIT_COMMIT_SHORT}"
echo "BUILD CREATOR: ${BUILD_CREATOR}"
echo "BUILD NAME: ${DOCKER_IMAGE}:${GIT_BRANCH}"

export GIT_BRANCH_LOWERCASE=$(echo "${GIT_BRANCH}" | awk '{print tolower($0)}'| sed 's/\//_/;')
DOCKER_ARGS=" --no-cache"

if [[ "$GIT_TAG" != "" ]]; then
     DOCKER_ARGS="${DOCKER_ARGS} \
         -t ${DOCKER_IMAGE}:${GIT_TAG}"
elif [[ "${GIT_BRANCH_LOWERCASE}" == "master" ]]; then
     DOCKER_ARGS="${DOCKER_ARGS} \
         -t ${DOCKER_IMAGE}:${GIT_BRANCH_LOWERCASE} -t ${DOCKER_IMAGE}:latest"
else
     DOCKER_ARGS="${DOCKER_ARGS} \
         -t ${DOCKER_IMAGE}:${GIT_BRANCH_LOWERCASE}"
fi

DOCKER_ARGS=" ${DOCKER_ARGS} \
    --build-arg GIT_BRANCH="$GIT_BRANCH" \
    --build-arg GIT_COMMIT="$GIT_COMMIT" \
    --build-arg TAG="${DOCKER_IMAGE}:${BUILD_TAG}" \
    --build-arg BUILD_CREATOR="$BUILD_CREATOR" \
    --build-arg VERSION="$VERSION" \
    --build-arg APPNAME="$APPNAME" \
    -f app/dockerfile \
    app/."

echo "args: ${DOCKER_ARGS}"
set -x
docker build ${DOCKER_ARGS}
set +x

echoTitle "Build done"
