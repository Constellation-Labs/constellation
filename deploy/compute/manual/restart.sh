#!/usr/bin/env bash

HOSTS_FILE=${1:-hosts-dev.txt}
JAR_TAG=${2:-dev}

echo "Restarting $HOSTS_FILE with jar tag $JAR_TAG"
cat $HOSTS_FILE

pscp -h $HOSTS_FILE ./deploy/compute/run.sh /home/$USER/
pssh -h $HOSTS_FILE -i 'killall bash'
pssh -h $HOSTS_FILE -i 'killall java'
pssh -h $HOSTS_FILE -i 'rm -rf /home/$USER/tmp'
pssh -h $HOSTS_FILE -i 'mkdir /home/$USER/tmp'

pssh -t 10 -h $HOSTS_FILE -i "export JAR_TAG=$JAR_TAG; nohup ./run.sh 2>&1 & disown"
#pssh -h $HOSTS_FILE -i "source ~/.ssh/environment; java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.authenticate=false -Dm.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$(cat external_host_ip) -Dcom.sun.management.jmxremote.local.only=false -jar ~/dag-$JAR_TAG.jar > ~/dag.log 2>&1 &" && \
echo "Done restarting"

export HOSTS_FILE=$HOSTS_FILE
sbt "it:testOnly org.constellation.ClusterComputeManualTest"