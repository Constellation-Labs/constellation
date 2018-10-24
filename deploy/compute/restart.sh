#!/usr/bin/env bash

pssh -h hosts.txt -i 'killall java'
pssh -h hosts.txt -i 'rm -rf /home/$USER/tmp'
pssh -h hosts.txt -i 'source ~/.ssh/environment; java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.authenticate=false -Dm.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$(cat external_host_ip) -Dcom.sun.management.jmxremote.local.only=false -jar ~/dag.jar > ~/dag.log 2>&1 &' && \
sbt "it:testOnly org.constellation.ClusterComputeManualTest"