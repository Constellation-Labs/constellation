#!/usr/bin/env bash

DEST=$1
BRANCH=$2

pushd ~/constellation
git fetch origin && git reset --hard $BRANCH && ./assemble.sh && ../upload_jar.sh $DEST
popd
