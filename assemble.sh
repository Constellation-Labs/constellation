#!/usr/bin/env bash
pushd ui
./build.sh
# Redundant but unclear why issue of not updating exists sometimes
./copy.sh
popd
sbt assembly
