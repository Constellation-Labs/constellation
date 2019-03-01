#!/usr/bin/env bash
mkdir -p src/main/resources/ui/
rm ../src/main/resources/ui/*js*
sbt clean fullOptJS
# Unclear why this copy fails sometimes?
cp ./target/scala-2.12/*js* ../src/main/resources/ui/
cp ./target/scala-2.12/*js* ../src/main/resources/ui/
cp ./target/scala-2.12/*js* ../src/main/resources/ui/
