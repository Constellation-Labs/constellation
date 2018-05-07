#!/usr/bin/env bash


GOOGLE_PROJECT_ID="esoteric-helix-197319"
GOOGLE_CLUSTER_NAME="constellation-test"
DATE=$(date +%s)
echo $DATE
IMAGE_TAG=$USER-$DATE
IMAGE=gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$IMAGE_TAG
echo $IMAGE

sbt docker:publishLocal
docker tag constellationlabs/constellation:latest $IMAGE
gcloud docker -- push $IMAGE


kubectl patch statefulset constellation-app --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"'$IMAGE'"}]'


APP_USER="constellation-app-$USER"
echo $APP_USER

STRING=$(kubectl get sts $APP_USER 2>&1)
echo "string" $STRING

if [[ $STRING = *"Error"* ]]; then
    echo "No cluster found for $USER - deploying new STS"
    cp ./deploy/kubernetes/node-deployment.yml ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation-app/constellation-app-$USER/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/node-service/node-service-$USER/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation:latest/constellation:$IMAGE_TAG/g" ./deploy/kubernetes/node-deployment-impl.yml
    kubectl apply -f ./deploy/kubernetes/node-deployment-impl.yml
    rm ./deploy/kubernetes/node-deployment-impl.yml
else
    echo "Pre-existing cluster found for $USER, patching STS"
fi
