#!/usr/bin/env bash

echo "Provisioning vagrant vm for constellation minikube host support."
echo "Note this is intended to be run through Vagrant (with 'vagrant up' cmd)."

# fail if any commands below fail
set -e

echo "Checking we are in a vagrant environment.................."
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$SCRIPT_DIR/is-vagrant.sh

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

# install minikube
# install minikube
echo "Installing minikube.........................."
curl -Lo minikube https://storage.googleapis.com/minikube/releases/v0.25.0/minikube-linux-amd64
chmod +x minikube
MINIKUBE_MD5_EXPECTED="c4b0f3a282efe690f554bdc7d93dbaae"
MINIKUBE_MD5_ACTUAL=$(md5sum minikube)
if [ $MINIKUBE_MD5_ACTUAL -ne $MINIKUBE_MD5_EXPECTED ]; then
   echo "md5sum of minikube did not match expected checksum. Exiting";
   exit 1;
fi

# docker local publish
sudo mv minikube /usr/local/bin/
echo "minikube machine setup complete!"

# docker local publish
cd /home/vagrant/constellation
sudo sbt docker:publishLocal
