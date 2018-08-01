pushd ui
sbt fullOptJS
popd
cp /Users/tyler/constellation/constellation/ui/target/scala-2.11/*js* src/main/resources/ui/
sbt run
