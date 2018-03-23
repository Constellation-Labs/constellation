#!/usr/bin/env bash

vagrant ssh -c 'cd /home/vagrant/constellation; sudo sbt docker:publishLocal'
