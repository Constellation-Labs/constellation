#!/usr/bin/env bash
rm ../src/main/resources/ui/*js*
sbt fullOptJS
cp ./target/scala-2.12/*js* ../src/main/resources/ui/
