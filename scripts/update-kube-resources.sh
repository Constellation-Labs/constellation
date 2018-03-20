#!/usr/bin/env bash

vagrant ssh -c 'sudo kubectl apply -R -f /home/vagrant/constellation/src/main/resources/'
