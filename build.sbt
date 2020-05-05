import Regression._
import sbt.Keys.mainClass

envVars in Test := Map("CL_STOREPASS" -> "storepass", "CL_KEYPASS" -> "keypass")
enablePlugins(JavaAgent, JavaAppPackaging)
addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

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

lazy val _version = "2.5.8"

lazy val commonSettings = Seq(
  version := _version,
  scalaVersion := "2.12.10",
  organization := "org.constellation"
)

lazy val coreSettings = Seq(
  parallelExecution in Test := false,
  resolvers += "Artima Maven Repository".at("https://repo.artima.com/releases"),
  resolvers += "Typesafe Releases".at("https://repo.typesafe.com/typesafe/maven-releases/"),
  resolvers += "jitpack".at("https://jitpack.io"),
  resolvers += Resolver.bintrayRepo("abankowski", "maven")
)

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
  val http4s = "0.21.2"
  val circe = "0.12.3"
  val circeEnumeratum = "1.5.23"
  val circeGenericExtras = "0.13.0"
}

lazy val http4sDependencies = Seq(
  "org.http4s" %% "http4s-blaze-server",
  "org.http4s" %% "http4s-blaze-client",
  "org.http4s" %% "http4s-circe",
  "org.http4s" %% "http4s-dsl",
  "org.http4s" %% "http4s-prometheus-metrics",
  "org.http4s" %% "http4s-okhttp-client"
).map(_ % versions.http4s)

lazy val circeDependencies = Seq(
  "io.circe" %% "circe-core" % versions.circe,
  "io.circe" %% "circe-generic" % versions.circe,
  "io.circe" %% "circe-generic-extras" % versions.circeGenericExtras,
  "io.circe" %% "circe-parser" % versions.circe,
  "com.beachape" %% "enumeratum-circe" % versions.circeEnumeratum
)

lazy val sharedDependencies = Seq(
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  ("org.typelevel" %% "cats-core" % versions.cats).withSources().withJavadoc(),
  ("org.typelevel" %% "cats-effect" % versions.cats).withSources().withJavadoc(),
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "io.chrisdavenport" %% "log4cats-slf4j" % "1.0.0"
) ++ circeDependencies

lazy val keyToolSharedDependencies = Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.91.0",
  "com.madgag.spongycastle" % "core" % versions.spongyCastle,
  "com.madgag.spongycastle" % "prov" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpg-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bctls-jdk15on" % versions.spongyCastle,
  "org.bouncycastle" % "bcprov-jdk15on" % "1.65"
) ++ sharedDependencies

lazy val walletSharedDependencies = Seq(
  "com.twitter" %% "chill" % versions.twitterChill,
  "com.twitter" %% "algebird-core" % "0.13.5"
) ++ sharedDependencies

lazy val schemaSharedDependencies = keyToolSharedDependencies ++ walletSharedDependencies

lazy val playgroundDependencies = Seq(
  "io.higherkindness" %% "droste-core" % "0.8.0"
) ++
  sharedDependencies

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
  "org.perf4j" % "perf4j" % "0.9.16",
  "pl.abankowski" %% "http-request-signer-core" % "0.3.2",
  "pl.abankowski" %% "http4s-request-signer" % "0.3.2"
) ++ http4sDependencies ++ schemaSharedDependencies

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
).map(_ % "it,test,regression")

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

lazy val root = (project in file("."))
  .dependsOn(schema)
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .configs(IntegrationTest)
  .configs(RegressionTest)
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
    name := "constellation",
    coreSettings,
    Regression.regressionSettings,
    Defaults.itSettings,
    libraryDependencies ++= (coreDependencies ++ testDependencies),
    mainClass := Some("org.constellation.ConstellationNode")
    // other settings here
  )

lazy val keytool = (project in file("keytool"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    name := "keytool",
    buildInfoKeys := Seq[BuildInfoKey](
      version
    ),
    buildInfoPackage := "org.constellation.keytool",
    buildInfoOptions ++= Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToMap),
    mainClass := Some("org.constellation.keytool.KeyTool"),
    libraryDependencies ++= keyToolSharedDependencies
  )

lazy val wallet = (project in file("wallet"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(keytool)
  .settings(
    commonSettings,
    name := "wallet",
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

lazy val playground = (project in file("playground"))
  .settings(
    libraryDependencies ++= playgroundDependencies ++ testDependencies
  )
