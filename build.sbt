import E2E._
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import sbt.Keys.mainClass

envVars in Test := Map("CL_STOREPASS" -> "storepass", "CL_KEYPASS" -> "keypass")
enablePlugins(JavaAgent, JavaAppPackaging)
//addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")

scalacOptions :=
  Seq(
    "-Ypartial-unification",
    "-unchecked",
    "-deprecation",
    "-feature",
    "-language:postfixOps",
    "-language:implicitConversions",
    "-language:higherKinds"
  )
javaAgents += "org.aspectj" % "aspectjweaver" % "1.9.4" % "runtime"

// javacOptions := Seq("-XX:MaxMetaspaceSize=256m")

lazy val _version = "1.0.12"

lazy val versions = new {
  val akka = "2.5.25"
  val akkaHttp = "10.1.9"
  val akkaHttpCors = "0.4.1"
  val spongyCastle = "1.58.0.0"
  val micrometer = "1.2.1"
  val prometheus = "0.6.0"
  val sttp = "1.6.6"
  val cats = "2.0.0"
  val json4s = "3.6.7"
  val mockito = "1.5.16"
  val twitterChill = "0.9.3"
}

lazy val sttpDependencies = Seq(
  "com.softwaremill.sttp" %% "okhttp-backend" % versions.sttp,
  "com.softwaremill.sttp" %% "json4s" % versions.sttp,
  "com.softwaremill.sttp" %% "prometheus-backend" % versions.sttp
)

lazy val commonSettings = Seq(
  version := _version,
  scalaVersion := "2.12.10",
  organization := "org.constellation",
  name := "constellation"
)

lazy val coreSettings = Seq(
  parallelExecution in Test := false,
  dockerBaseImage := "openjdk:8-jdk",
  dockerExposedPorts := Seq(2551, 9000, 6006, 9010, 9001, 9002),
  dockerExposedUdpPorts := Seq(16180),
  dockerCommands := dockerCommands.value.flatMap {
    case ExecCmd("ENTRYPOINT", args @ _*) => Seq(Cmd("ENTRYPOINT", args.mkString(" ")))
    case v                                => Seq(v)
  },
  dockerCommands += Cmd(
    "HEALTHCHECK",
    "--interval=30s",
    "--timeout=3s",
    "CMD",
    "curl -f http://localhost:9000/health || exit 1"
  ),
  dockerUsername := Some("constellationlabs"),
  dockerAlias := DockerAlias(
    None,
    Some("constellationlabs"),
    "constellation",
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
  resolvers += "Artima Maven Repository".at("https://repo.artima.com/releases"),
  resolvers += "Typesafe Releases".at("https://repo.typesafe.com/typesafe/maven-releases/"),
  resolvers += "jitpack".at("https://jitpack.io")
)

lazy val sharedDependencies = Seq(
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  ("org.typelevel" %% "cats-core" % versions.cats).withSources().withJavadoc(),
  ("org.typelevel" %% "cats-effect" % versions.cats).withSources().withJavadoc(),
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "io.chrisdavenport" %% "log4cats-slf4j" % "1.0.0"
)

lazy val keyToolSharedDependencies = Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.91.0",
  "com.madgag.spongycastle" % "core" % versions.spongyCastle,
  "com.madgag.spongycastle" % "prov" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpg-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bctls-jdk15on" % versions.spongyCastle
//  "org.bouncycastle" % "bcprov-jdk15on" % "1.63"
) ++ sharedDependencies

lazy val walletSharedDependencies = Seq(
  "com.twitter" %% "chill" % versions.twitterChill,
  "com.twitter" %% "algebird-core" % "0.13.5",
  "org.json4s" %% "json4s-native" % versions.json4s,
  "org.json4s" %% "json4s-ext" % versions.json4s,
  "org.json4s" %% "json4s-jackson" % versions.json4s,
  "org.json4s" %% "json4s-ast" % versions.json4s
) ++ sharedDependencies

lazy val schemaSharedDependencies = keyToolSharedDependencies ++ walletSharedDependencies

lazy val coreDependencies = Seq(
  ("com.github.pathikrit" %% "better-files" % "3.8.0").withSources().withJavadoc(),
  "org.scala-lang.modules" %% "scala-async" % "0.10.0",
  "com.roundeights" %% "hasher" % "1.2.0",
  "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-remote" % versions.akka,
  "com.typesafe.akka" %% "akka-slf4j" % versions.akka,
  "ch.megard" %% "akka-http-cors" % versions.akkaHttpCors,
  "de.heikoseeberger" %% "akka-http-json4s" % "1.27.0",
  //  "org.typelevel" %% "alleycats-core" % versions.cats withSources() withJavadoc(),
  "net.glxn" % "qrgen" % "1.4",
  //  "com.softwaremill.macmemo" %% "macros" % "0.4" withJavadoc() withSources(),
  "com.twitter" %% "storehaus-cache" % "0.15.0",
  "io.swaydb" %% "swaydb" % "0.7.1",
  "io.micrometer" % "micrometer-registry-prometheus" % versions.micrometer,
  "io.kontainers" %% "micrometer-akka" % "0.10.2",
  "io.prometheus" % "simpleclient" % versions.prometheus,
  "io.prometheus" % "simpleclient_common" % versions.prometheus,
  "io.prometheus" % "simpleclient_caffeine" % versions.prometheus,
  "io.prometheus" % "simpleclient_logback" % versions.prometheus,
  "com.github.java-json-tools" % "json-schema-validator" % "2.2.11",
  "com.github.japgolly.scalacss" %% "ext-scalatags" % "0.5.6",
  "com.github.djelenc" % "alpha-testbed" % "1.0.3",
  ("com.github.blemale" %% "scaffeine" % "3.1.0").withSources().withJavadoc(),
  ("com.typesafe.slick" %% "slick" % "3.3.2").withSources().withJavadoc(),
  "com.h2database" % "h2" % "1.4.199",
  "net.logstash.logback" % "logstash-logback-encoder" % "5.1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.665",
  "org.perf4j" % "perf4j" % "0.9.16"
) ++ sttpDependencies ++ schemaSharedDependencies

//Test dependencies
lazy val testDependencies = Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.0",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalamock" %% "scalamock" % "4.4.0",
  "org.mockito" %% "mockito-scala" % versions.mockito,
  "org.mockito" %% "mockito-scala-cats" % versions.mockito,
  "com.typesafe.akka" %% "akka-http-testkit" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-testkit" % versions.akka
).map(_ % "it,test,e2e")

testOptions in Test += Tests.Setup(() => System.setProperty("macmemo.disable", "true"))
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-results/scalatest")

test in assembly := {}

Test / fork := true // <-- comment out to attach debugger
Test / logBuffered := false

assemblyMergeStrategy in assembly := {
  case "logback.xml"                                       => MergeStrategy.first
  case x if x.contains("io.netty.versions.properties")     => MergeStrategy.discard
  case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val protobuf = (project in file("proto"))
  .settings(
    commonSettings,
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val root = (project in file("."))
  .dependsOn(protobuf, schema)
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .configs(IntegrationTest)
  .configs(E2ETest)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      sbtVersion,
      "gitBranch" -> git.gitCurrentBranch.value,
      "gitCommit" -> git.gitHeadCommit.value.getOrElse("commit N/A")
    ),
    buildInfoPackage := "org.constellation",
    buildInfoOptions ++= Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToMap),
    commonSettings,
    coreSettings,
    E2E.e2eSettings,
    Defaults.itSettings,
    libraryDependencies ++= (coreDependencies ++ testDependencies),
    mainClass := Some("org.constellation.ConstellationNode")
    // other settings here
  )

lazy val keytool = (project in file("keytool"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      version
    ),
    buildInfoPackage := "org.constellation.keytool",
    buildInfoOptions ++= Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToMap),
    mainClass := Some("org.constellation.keytool.KeyTool"),
    unmanagedJars in Runtime += file("core-1.64.jar"),
    unmanagedJars in Runtime += file("prov-1.64.jar"),
    libraryDependencies ++= keyToolSharedDependencies
  )

lazy val wallet = (project in file("wallet"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(keytool)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      version
    ),
    buildInfoPackage := "org.constellation.wallet",
    buildInfoOptions ++= Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToMap),
    mainClass := Some("org.constellation.wallet.Wallet"),
    libraryDependencies ++= walletSharedDependencies
  )

lazy val schema = (project in file("schema"))
  .dependsOn(keytool)
  .dependsOn(wallet)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      version
    ),
    buildInfoPackage := "org.constellation.schema",
    buildInfoOptions ++= Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToMap),
    libraryDependencies ++= schemaSharedDependencies
  )
