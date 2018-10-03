#!/usr/bin/env bash


#Ubuntu 1404

sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install openjdk-8-jre

# For snapshot creation ^


Host *
    StrictHostKeyChecking no

~/.ssh/config

chmod 400 ~/.ssh/config





java -jar ~/constellation-assembly-1.0.1.jar > ~/dag.log 2>&1 &



