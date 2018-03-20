#!/usr/bin/env bash

VAGRANT_HOME="/home/vagrant/constellation"

if [ -d $VAGRANT_HOME ]; then
    exit 0
fi

echo "Did not detect vagrant environment, i.e. folder missing: $VAGRANT_HOME"
exit 1
