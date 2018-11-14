#!/usr/bin/env bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

HOSTS_FILE=${1:-hosts-dev.txt}
JAR_TAG=${2:-dev}

echo "Redeploying $HOSTS_FILE"
cat $HOSTS_FILE

source $DIR/update-jar.sh $HOSTS_FILE $JAR_TAG && \
source $DIR/service-restart.sh $HOSTS_FILE