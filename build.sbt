name := "constellation"

version := "0"

scalaVersion := "2.12.2"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

enablePlugins(JavaAppPackaging)

dockerExposedPorts := Seq(9000, 2552)

lazy val versions = new {
  val akka = "2.4.18"
  val akkaHttp = "10.0.7"
}

libraryDependencies ++= Seq(
  "com.roundeights" %% "hasher" % "1.2.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-remote" % versions.akka,
  "de.heikoseeberger" %% "akka-http-json4s" % "1.16.1",
  "org.json4s" %% "json4s-native" % "3.5.2",
  "net.liftweb" %% "lift-json" % "3.1.1",
  "com.madgag.spongycastle" % "core" % "1.58.0.0",
  "com.madgag.spongycastle" % "prov" % "1.58.0.0",
 // "com.madgag.spongycastle" % "pkix" % "1.58.0.0",
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % "1.58.0.0",
  //"com.madgag.spongycastle" % "pg" % "1.58.0.0",
  "com.madgag.spongycastle" % "bcpg-jdk15on" % "1.58.0.0",
  "com.madgag.spongycastle" % "bctls-jdk15on" % "1.58.0.0",
  "net.liftweb" %% "lift-json" % "3.1.1"
)

//Test dependencies
libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.13.4",
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0",
  "com.typesafe.akka" %% "akka-http-testkit" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-testkit" % versions.akka
).map(_ % "test" )

mainClass := Some("org.constellation.BlockChainApp")

parallelExecution in Test := false