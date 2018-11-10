#!/usr/bin/env bash

# Install google-cloud-sdk
# This is only for creating a snapshot machine image
# Run manually

pssh -h hosts.txt -i sudo add-apt-repository -y ppa:webupd8team/java
pssh -h hosts.txt -i sudo apt-get update
pssh -h hosts.txt -i 'echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections'
pssh -h hosts.txt -i sudo apt-get install -y oracle-java8-installer haveged
