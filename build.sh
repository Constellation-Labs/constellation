pushd ui
sbt fullOptJS
popd
cp ui/target/scala-2.11/*js* src/main/resources/ui/
sbt run
