#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts.txt}

echo "Redeploying $HOSTS_FILE"
cat $HOSTS_FILE

sbt assembly && \
gsutil cp target/scala-2.12/constellation-assembly-1.0.1.jar gs://constellation-dag/release/dag.jar && \
gsutil acl ch -u AllUsers:R gs://constellation-dag/release/dag.jar && \
pssh -h $HOSTS_FILE -i 'gsutil cp gs://constellation-dag/release/dag.jar .' && \
pssh -h $HOSTS_FILE -i 'killall java'
pssh -h $HOSTS_FILE -i "rm -rf /home/$USER/tmp"
pssh -h $HOSTS_FILE -i 'source ~/.ssh/environment; java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.authenticate=false -Dm.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$(cat external_host_ip) -Dcom.sun.management.jmxremote.local.only=false -jar ~/dag.jar > ~/dag.log 2>&1 &' && \
echo "Done"
sbt "it:testOnly org.constellation.ClusterComputeManualTest"