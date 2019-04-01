import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

enablePlugins(JavaAppPackaging)
//addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

scalacOptions := Seq("-Ypartial-unification", "-unchecked", "-deprecation")

// javacOptions := Seq("-XX:MaxMetaspaceSize=256m")


lazy val _version = "1.0.12"

lazy val versions = new {
  val akka = "2.5.21"
  val akkaHttp = "10.1.8"
  val akkaHttpCors = "0.4.0"
  val spongyCastle = "1.58.0.0"
  val micrometer = "1.1.3"
  val prometheus = "0.6.0"
  val sttp = "1.5.11"
  val cats = "1.6.0"
  val json4s = "3.6.5"
}

lazy val sttpDependencies = Seq(
  "com.softwaremill.sttp" %% "okhttp-backend" % versions.sttp,
  "com.softwaremill.sttp" %% "json4s" % versions.sttp,
  "com.softwaremill.sttp" %% "prometheus-backend" % versions.sttp
)

lazy val commonSettings = Seq(
  version := _version,
  scalaVersion := "2.12.8",
  organization := "org.constellation",
  name := "constellation",
  mainClass := Some("org.constellation.ConstellationNode"),
  parallelExecution in Test := false,
  dockerBaseImage := "openjdk:8-jdk",
  dockerExposedPorts := Seq(2551, 9000, 6006, 9010, 9001, 9002),
  dockerExposedUdpPorts := Seq(16180),
  dockerCommands := dockerCommands.value.flatMap {
    case ExecCmd("ENTRYPOINT", args @ _*) => Seq(Cmd("ENTRYPOINT", args.mkString(" ")))
    case v                                => Seq(v)
  },
  dockerCommands += Cmd("HEALTHCHECK",
                        "--interval=30s",
                        "--timeout=3s",
                        "CMD",
                        "curl -f http://localhost:9000/health || exit 1"),
  dockerUsername := Some("constellationlabs"),
  dockerAlias := DockerAlias(None,
                             Some("constellationlabs"),
                             "constellation",
                             Some(sys.env.getOrElse("CIRCLE_SHA1", _version))),
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
  "com.github.pathikrit" %% "better-files" % "3.7.1" withSources() withJavadoc(),
  "com.roundeights" %% "hasher" % "1.2.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-remote" % versions.akka,
  "ch.megard" %% "akka-http-cors" % versions.akkaHttpCors,
  "de.heikoseeberger" %% "akka-http-json4s" % "1.25.2",
  "org.json4s" %% "json4s-native" % versions.json4s,
  "org.json4s" %% "json4s-ext" % versions.json4s,
  "org.json4s" %% "json4s-jackson" % versions.json4s,
  "org.json4s" %% "json4s-ast" % versions.json4s,
  "com.madgag.spongycastle" % "core" % versions.spongyCastle,
  "com.madgag.spongycastle" % "prov" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpg-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bctls-jdk15on" % versions.spongyCastle,
  "org.bouncycastle" % "bcprov-jdk15on" % "1.61",
  "com.twitter" %% "chill" % "0.9.3",
  "com.twitter" %% "algebird-core" % "0.13.5",
  "org.typelevel" %% "cats-core" % versions.cats withSources () withJavadoc (),
//  "org.typelevel" %% "alleycats-core" % versions.cats withSources() withJavadoc(),
  "org.typelevel" %% "cats-effect" % "1.2.0" withSources() withJavadoc(),
  "net.glxn" % "qrgen" % "1.4",
//  "com.softwaremill.macmemo" %% "macros" % "0.4" withJavadoc() withSources(),
  "com.twitter" %% "storehaus-cache" % "0.15.0",
  "io.swaydb" %% "swaydb" % "0.7.1",
  "io.micrometer" % "micrometer-registry-prometheus" % versions.micrometer,
  "io.prometheus" % "simpleclient" % versions.prometheus,
  "io.prometheus" % "simpleclient_common" % versions.prometheus,
  "io.prometheus" % "simpleclient_caffeine" % versions.prometheus,
  "io.prometheus" % "simpleclient_logback" % versions.prometheus,
  "com.github.java-json-tools" % "json-schema-validator" % "2.2.10",
  "com.github.japgolly.scalacss" %% "ext-scalatags" % "0.5.5",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "com.github.blemale" %% "scaffeine" % "2.6.0" withSources() withJavadoc()
) ++ sttpDependencies

//Test dependencies
lazy val testDependencies = Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.0",
  "org.scalatest" %% "scalatest" % "3.0.7",
  "org.scalactic" %% "scalactic" % "3.0.7",
  "org.scalamock" %% "scalamock" % "4.1.0",
  "com.typesafe.akka" %% "akka-http-testkit" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-testkit" % versions.akka
).map(_ % "it,test")

testOptions in Test += Tests.Setup(() => System.setProperty("macmemo.disable", "true"))
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,"-u", "target/test-results/scalatest")

test in assembly := {}

Test / fork := true
Test / logBuffered := false

assemblyMergeStrategy in assembly := {
  case "logback.xml" => MergeStrategy.first
  case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val root = (project in file("."))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .configs(IntegrationTest)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "org.constellation",
    buildInfoOptions ++= Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToJson),
    commonSettings,
    Defaults.itSettings,
    libraryDependencies ++= (coreDependencies ++ testDependencies)
    // other settings here
  )
