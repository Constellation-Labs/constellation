#!/usr/bin/env bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

JAR_TAG=${1:-dev}
NODE_COUNT=${2:-3}
DEPLOY_DIR=${3:-custom-image}

##source ${DIR}/assemble-upload.sh ${JAR_TAG} && \
source ${DIR}/start-cluster.sh ${JAR_TAG} ${NODE_COUNT}
source ${DIR}/restart.sh ./terraform/${DEPLOY_DIR}-${JAR_TAG}/hosts
