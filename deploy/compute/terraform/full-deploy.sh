#!/usr/bin/env bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

JAR_TAG=${1:-dev}
NODE_COUNT=${2:-3}

pushd ui
./build.sh
# Redundant but unclear why issue of not updating exists sometimes
./copy.sh
popd

sbt clean assembly

pushd terraform

DEPLOY_FOLDER=./default-$JAR_TAG
rm -r $DEPLOY_FOLDER
cp -r ./default $DEPLOY_FOLDER

pushd $DEPLOY_FOLDER

sed -i'.bak' "s/constellation-app/$APP_USER/g" ./deploy/kubernetes/node-deployment-impl.yml


#source $DIR/deploy.sh $HOSTS_FILE $JAR_TAG
