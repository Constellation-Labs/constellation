#!/usr/bin/env bash

echo "Provisioning vagrant vm for constellation."
echo "Note this is intended to be run through Vagrant (with 'vagrant up' cmd)."

# fail if any commands below fail
set -e

echo "Starting machine setup.........................."

# install openjdk java8
echo "Installing java8.........................."
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install -y openjdk-8-jdk

# install scala
echo "Installing scala.........................."
wget https://downloads.lightbend.com/scala/2.12.4/scala-2.12.4.deb
sudo dpkg -i scala-2.12.4.deb

# install sbt
sudo apt-get install -y apt-transport-https
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install -y sbt

# https://bugs.launchpad.net/ubuntu/+source/ca-certificates-java/+bug/1396760
sudo /var/lib/dpkg/info/ca-certificates-java.postinst configure

#install git
echo "Installing git.........................."
sudo apt-get install -y git

# install docker
echo "Installing docker.........................."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install -y docker-ce

# install kubectl
echo "Installing kubectl.........................."
wget https://storage.googleapis.com/kubernetes-release/release/v1.9.0/bin/linux/amd64/kubectl
chmod +x kubectl

KUBECTL_MD5_EXPECTED="01dce19bf06f7d49772a3cf687b6b586"
KUBECTL_MD5_ACTUAL=$(md5sum kubectl)
if [ $KUBECTL_MD5_ACTUAL -ne $KUBECTL_MD5 ]; then
   echo "md5sum of kubectl did not match expected checksum. Exiting";
   exit 1;
fi
sudo mv kubectl /usr/local/bin/kubectl

# Install Google Cloud SDK
export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"

echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

sudo apt-get update && sudo apt-get install -y google-cloud-sdk

echo "machine setup complete!"

echo "publishing docker image locally"
# docker local publish
cd /home/vagrant/constellation
sudo sbt docker:publishLocal
