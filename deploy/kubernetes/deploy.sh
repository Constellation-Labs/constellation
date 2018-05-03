#!/usr/bin/env bash


for i in {1..3}
do
    echo "Restarting node number $i"
    sed -i "s/constellation-app/constellation-app-$i/g" ./deploy/kubernetes/node-deployment.yml
    sed -i "s/node-service/node-service-$i/g" ./deploy/kubernetes/node-deployment.yml
    sudo  /opt/google-cloud-sdk/bin/kubectl delete -f ./deploy/kubernetes/node-deployment.yml;
    sudo  /opt/google-cloud-sdk/bin/kubectl create -f ./deploy/kubernetes/node-deployment.yml;
    echo "Done"
done

