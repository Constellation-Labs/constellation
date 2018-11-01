#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts-dev.txt}
JAR_TAG=${2:-dev}

echo "Restarting $HOSTS_FILE with jar tag $JAR_TAG"
cat $HOSTS_FILE

pssh -h $HOSTS_FILE -i 'sudo rm -rf /home/ubuntu/constellation/tmp'
pssh -h $HOSTS_FILE -i "sudo systemctl restart constellation"

echo "Done restarting"

export HOSTS_FILE=$HOSTS_FILE
sbt "it:testOnly org.constellation.ClusterComputeManualTest"