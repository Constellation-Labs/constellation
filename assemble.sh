#!/usr/bin/env bash
pushd ui
sbt clean fullOptJS
popd
mkdir -p src/main/resources/ui/
cp ui/target/scala-2.11/*js* src/main/resources/ui/
sbt assembly
