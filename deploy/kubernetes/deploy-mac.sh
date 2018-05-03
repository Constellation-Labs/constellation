#!/usr/bin/env bash



GOOGLE_CLUSTER_NAME="constellation-test"
gcloud --quiet container clusters get-credentials $GOOGLE_CLUSTER_NAME

cp ./deploy/kubernetes/node-deployment.yml ./deploy/kubernetes/node-deployment-impl.yml

for i in {1..3}
do
    echo "Restarting node number $i"

    cp ./deploy/kubernetes/node-deployment.yml ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation-app/constellation-app-$i/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/node-service/node-service-$i/g" ./deploy/kubernetes/node-deployment-impl.yml
    kubectl delete -f ./deploy/kubernetes/node-deployment-impl.yml;
    kubectl create -f ./deploy/kubernetes/node-deployment-impl.yml;
    echo "Done"
done

rm ./deploy/kubernetes/node-deployment-impl.yml
rm ./deploy/kubernetes/node-deployment-impl*.yml