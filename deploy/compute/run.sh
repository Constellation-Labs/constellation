#!/usr/bin/env bash

(while true; do
    java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9011 -Dcom.sun.management.jmxremote.authenticate=false -Dm.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$(cat external_host_ip) -Dcom.sun.management.jmxremote.local.only=false -jar ~/dag-$JAR_TAG.jar > ~/dag.log 2>&1 ;
    sleep 5
done) &