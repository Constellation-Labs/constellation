#!/usr/bin/env bash

GOOGLE_PROJECT_ID="esoteric-helix-197319"
#GOOGLE_CLUSTER_NAME="constellation-test"
DATE=$(date +%s)

# This is required because unless the YAML changes a value it won't trigger a redeploy to a new docker image..
IMAGE_TAG=$USER-$DATE
IMAGE=gcr.io/$GOOGLE_PROJECT_ID/constellationlabs/constellation:$IMAGE_TAG
echo "Using docker image: $IMAGE"

sbt docker:publishLocal
docker tag constellationlabs/constellation:latest $IMAGE
gcloud docker -- push $IMAGE

USER_STR="$USER"
APP_USER="constellation-app-$USER_STR"
echo $APP_USER

STRING=$(kubectl get sts $APP_USER 2>&1)
echo "kubectl response to get sts on $APP_USER: $STRING"

if [[ $STRING = *"Error"* ]]; then
    echo "No cluster found for $APP_USER - deploying new STS"
    cp ./deploy/kubernetes/node-deployment.yml ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation-app/$APP_USER/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/node-service/node-service-$USER_STR/g" ./deploy/kubernetes/node-deployment-impl.yml
    sed -i'.bak' "s/constellation:latest/constellation:$IMAGE_TAG/g" ./deploy/kubernetes/node-deployment-impl.yml
    kubectl apply -f ./deploy/kubernetes/node-deployment-impl.yml
    # Re-enable rm later -- leave file around for debugging for now.
    # rm ./deploy/kubernetes/node-deployment-impl.yml
else
    echo "Pre-existing cluster found for $USER_STR, patching STS"
    kubectl patch statefulset $APP_USER --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"'$IMAGE'"}]'
fi


kubectl rollout status sts $APP_USER

export CLUSTER_ID=$APP_USER

sbt it:test