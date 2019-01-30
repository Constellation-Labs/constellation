import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

enablePlugins(JavaAppPackaging)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

scalacOptions := Seq("-unchecked", "-deprecation")

logBuffered in Test := false

lazy val _version = "1.0.1"

lazy val versions = new {
  val akka = "2.5.19"
  val akkaHttp = "10.1.7"
  val akkaHttpCors = "0.3.4"
  val spongyCastle = "1.58.0.0"
  val micrometer = "1.1.2"
  val prometheus  = "0.6.0"
  val sttp = "1.5.7"
}

lazy val sttpDependencies = Seq(
  "com.softwaremill.sttp" %% "okhttp-backend" % versions.sttp,
  "com.softwaremill.sttp" %% "json4s" % versions.sttp,
  "com.softwaremill.sttp" %% "prometheus-backend" % versions.sttp
)

lazy val commonSettings = Seq(
  version := _version,
  scalaVersion := "2.12.7",
  organization := "org.constellation",
  name := "constellation",
  mainClass := Some("org.constellation.ConstellationNode"),
  parallelExecution in Test := false,
  dockerBaseImage := "openjdk:8-jdk",
  dockerExposedPorts := Seq(2551, 9000, 6006, 9010, 9001, 9002),
  dockerExposedUdpPorts := Seq(16180),
  dockerCommands := dockerCommands.value.flatMap {
    case ExecCmd("ENTRYPOINT", args @ _*) => Seq(Cmd("ENTRYPOINT", args.mkString(" ")))
    case v => Seq(v)
  },
  dockerCommands += Cmd("HEALTHCHECK", "--interval=30s", "--timeout=3s", "CMD", "curl -f http://localhost:9000/health || exit 1"),
  dockerUsername := Some("constellationlabs"),
  dockerAlias := DockerAlias(None, Some("constellationlabs"), "constellation",
    Some(sys.env.getOrElse("CIRCLE_SHA1", _version))
  ),
  // Update the latest tag when publishing
  dockerUpdateLatest := true,
  // These values will be filled in by the k8s StatefulSet and Deployment
  dockerEntrypoint ++= Seq(
    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6006",
    "-Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false",
    """-DakkaActorSystemName="$AKKA_ACTOR_SYSTEM_NAME""""
  ),
  resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",
  resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/",
  resolvers += "jitpack" at "https://jitpack.io"
)

lazy val coreDependencies = Seq(
  "org.scala-lang.modules" %% "scala-async" % "0.9.7",
  "com.github.pathikrit" %% "better-files" % "3.7.0" withSources() withJavadoc(),
  "com.roundeights" %% "hasher" % "1.2.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-remote" % versions.akka,
  "ch.megard" %% "akka-http-cors" % versions.akkaHttpCors,
  "de.heikoseeberger" %% "akka-http-json4s" % "1.16.1",
  "org.json4s" %% "json4s-native" % "3.6.2",
  "org.json4s" %% "json4s-ext" % "3.6.2",
  "org.json4s" %% "json4s-jackson" % "3.6.2",
  "org.json4s" %% "json4s-ast" % "3.6.2",
  "com.madgag.spongycastle" % "core" % versions.spongyCastle,
  "com.madgag.spongycastle" % "prov" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpg-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bctls-jdk15on" % versions.spongyCastle,
  "org.bouncycastle" % "bcprov-jdk15on" % "1.51",
  "org.iq80.leveldb"            % "leveldb"          % "0.10" withSources() withJavadoc(),
  "com.codahale" % "shamir" % "0.6.0" withSources() withJavadoc(),
  "com.twitter" %% "chill" % "0.9.3",
  "com.twitter" %% "algebird-core" % "0.13.4",
  "org.typelevel" %% "cats-core" % "1.3.1",
  "org.typelevel" %% "alleycats-core" % "0.2.0",
  "net.glxn" % "qrgen" % "1.4",
  "com.softwaremill.macmemo" %% "macros" % "0.4" withJavadoc() withSources(),
  "com.typesafe.slick" %% "slick" % "3.2.3",
  "com.h2database" % "h2" % "1.4.197",
  "com.twitter" %% "storehaus-cache" % "0.15.0",
  "io.swaydb" %% "swaydb" % "0.6",
  "io.kontainers" %% "micrometer-akka" % "0.9.1",
  "io.micrometer" % "micrometer-registry-prometheus" % versions.micrometer,
  "io.prometheus" % "simpleclient" % versions.prometheus,
  "io.prometheus" % "simpleclient_common" % versions.prometheus,
  "com.github.java-json-tools" % "json-schema-validator" % "2.2.10"
) ++ sttpDependencies

//Test dependencies
lazy val testDependencies = Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.0",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalamock" %% "scalamock" % "4.1.0",
  "com.typesafe.akka" %% "akka-http-testkit" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-testkit" % versions.akka
).map(_ % "it,test" )

testOptions in Test += Tests.Setup(() => System.setProperty("macmemo.disable", "true"))

test in assembly := {}

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    Defaults.itSettings,
    libraryDependencies ++= (coreDependencies ++ testDependencies)
    // other settings here
  )
