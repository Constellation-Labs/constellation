#!/usr/bin/env bash


#Ubuntu 1404

sudo add-apt-repository ppa:openjdk-r/ppa -y
sudo apt-get update
sudo apt-get install -y openjdk-8-jre

# For snapshot creation ^


Host *
    StrictHostKeyChecking no

~/.ssh/config

chmod 400 ~/.ssh/config





java -jar ~/constellation-assembly-1.0.1.jar > ~/dag.log 2>&1 &



# Ubuntu 1804
sudo apt install open-jdk-8-jre-headless




sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer



pssh -h hosts.txt -i sudo add-apt-repository -y ppa:webupd8team/java
pssh -h hosts.txt -i sudo apt-get update
pssh -h hosts.txt -i 'echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections'
pssh -h hosts.txt -i sudo apt-get install -y oracle-java8-installer haveged

pssh -h hosts.txt -i 'ps aux | grep java'
pssh -h hosts.txt -i 'sudo killall java'
pssh -h hosts.txt -i 'java -jar ~/constellation-assembly-1.0.1.jar > ~/dag.log 2>&1 &'
pssh -h hosts.txt -i sudo apt-get install -y haveged
