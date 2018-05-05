#!/usr/bin/env bash


GOOGLE_PROJECT_ID="esoteric-helix-197319"
GOOGLE_CLUSTER_NAME="constellation-test"
IMAGE_TAG=$USER-test

sbt docker:publishLocal
docker tag constellationlabs/constellation:latest gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$IMAGE_TAG
gcloud --quiet container clusters get-credentials $GOOGLE_CLUSTER_NAME
gcloud docker -- push gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$IMAGE_TAG


for i in {1..3}
do
    echo "Restarting node number $i"

    cp ./deploy/kubernetes/node-deployment.yml ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation-app/constellation-app-$i/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/node-service/node-service-$i/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation:latest/constellation:$IMAGE_TAG/g" ./deploy/kubernetes/node-deployment-impl.yml
    kubectl delete -f ./deploy/kubernetes/node-deployment-impl.yml;
    kubectl create -f ./deploy/kubernetes/node-deployment-impl.yml;
    echo "Done"
done

rm ./deploy/kubernetes/node-deployment-impl.yml
rm ./deploy/kubernetes/node-deployment-impl.yml.bak

# kubectl rolling-update myapp --image=us.gcr.io/project-107012/myapp:5c3dda6b --image-pull-policy Always