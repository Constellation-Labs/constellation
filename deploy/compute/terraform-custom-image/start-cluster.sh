#!/usr/bin/env bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

JAR_TAG=${1:-dev}
NODE_COUNT=${2:-3}

ssh-add

pushd terraform

DEPLOY_FOLDER=./custom-image-${JAR_TAG}
echo "Starting $NODE_COUNT nodes with tag $JAR_TAG and deploy folder $DEPLOY_FOLDER"

mkdir ${DEPLOY_FOLDER}
pushd ${DEPLOY_FOLDER}
terraform destroy -auto-approve
popd

rm -r ${DEPLOY_FOLDER}
cp -r ./custom-image ${DEPLOY_FOLDER}

pushd ${DEPLOY_FOLDER}

sed -i'.bak' "s/state-default/state-default-$JAR_TAG/g" ./main.tf
sed -i'.bak' "s/instance_count = 3/instance_count = $NODE_COUNT/g" ./main.tf
sed -i'.bak' "s/dag-dev/dag-$JAR_TAG/g" ./setup.sh

terraform init -force-copy
terraform apply -auto-approve

./ips_to_hosts_file.sh > ./hosts

popd
popd