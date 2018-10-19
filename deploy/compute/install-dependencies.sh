#!/usr/bin/env bash


pssh -h hosts.txt -i sudo add-apt-repository -y ppa:webupd8team/java
pssh -h hosts.txt -i sudo apt-get update
pssh -h hosts.txt -i 'echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections'
pssh -h hosts.txt -i sudo apt-get install -y oracle-java8-installer haveged
