#!/usr/bin/env bash

pssh -h hosts.txt -i 'sudo killall java'
pssh -h hosts.txt -i 'rm -rf /home/ryle/tmp'
pssh -h hosts.txt -i 'java -jar ~/constellation-assembly-1.0.1.jar > ~/dag.log 2>&1 &'
sbt "it:testOnly org.constellation.ClusterComputeManualTest"