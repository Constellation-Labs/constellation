#!/usr/bin/env bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

HOSTS_FILE=${1:-hosts-dev.txt}
JAR_TAG=${2:-dev}

echo "Redeploying $HOSTS_FILE"
cat $HOSTS_FILE

sbt assembly && \
pssh -h $HOSTS_FILE -i 'sudo apt install -y google-cloud-sdk' # Snapshot GCP image didn't have this for some reason, remove later
gsutil cp target/scala-2.12/constellation-assembly-1.0.1.jar gs://constellation-dag/release/dag-$JAR_TAG.jar && \
gsutil acl ch -u AllUsers:R gs://constellation-dag/release/dag-$JAR_TAG.jar && \
pssh -h $HOSTS_FILE -i "gsutil cp gs://constellation-dag/release/dag-$JAR_TAG.jar ." && \

source $DIR/restart.sh $HOSTS_FILE $JAR_TAG
