#!/usr/bin/env bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

HOSTS_FILE=${1:-hosts-dev.txt}
JAR_TAG=${2:-dev}

echo "Redeploying $HOSTS_FILE"
cat $HOSTS_FILE

source $DIR/../build-upload.sh && \
pssh -h $HOSTS_FILE -i "sudo su ubuntu; gsutil cp gs://constellation-dag/release/dag-$JAR_TAG.jar /home/ubuntu/constellation/dag.jar" && \

source $DIR/restart.sh $HOSTS_FILE $JAR_TAG
