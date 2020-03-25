#!/usr/bin/env bash

sbt "++ 2.12.10" "keytool/assembly" "++ 2.12.10" "wallet/assembly"
mkdir -p ./src/main/resources/ui/
cd ui && sbt clean fullOptJS && cd ..
cp ./ui/target/scala-2.12/*js* ./src/main/resources/ui/
sbt "++ 2.12.10" assembly
