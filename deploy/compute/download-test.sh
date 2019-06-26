#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts2.txt}

echo "Redeploying $HOSTS_FILE"
cat $HOSTS_FILE

sbt assembly && \
pscp -h $HOSTS_FILE -O StrictHostKeyChecking=no target/scala-2.12/constellation-assembly-1.0.1.jar /home/$USER/ && \
pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i 'sudo killall java'
pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i 'rm -rf /home/$USER/tmp'
pssh -h $HOSTS_FILE -O StrictHostKeyChecking=no -i 'java -jar ~/constellation-assembly-1.0.1.jar > ~/dag.log 2>&1 &' && \
sbt "it:testOnly org.constellation.ClusterSingleDownloadJoinTest"