#!/usr/bin/env bash

echo "Publishing docker image to Artifactory"
docker login --username "${ARTIFACTORY_USER}" --password "${ARTIFACTORY_PWD}" "${DOCKER_REGISTRY}"
docker push "${DOCKER_IMAGE}"
