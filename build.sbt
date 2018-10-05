import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

enablePlugins(JavaAppPackaging)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

lazy val _version = "1.0.1"

lazy val versions = new {
  val akka = "2.5.16"
  val akkaHttp = "10.1.5"
  val akkaHttpCors = "0.3.0"
  val spongyCastle = "1.58.0.0"
}

lazy val commonSettings = Seq(
  version := _version,
  scalaVersion := "2.12.6",
  organization := "org.constellation",
  name := "constellation",
  mainClass := Some("org.constellation.ConstellationNode"),
  parallelExecution in Test := false,
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
/*
,
    """-Dakka.remote.netty.tcp.hostname="$(eval "echo $AKKA_REMOTING_BIND_HOST")"""",
    """-Dakka.remote.netty.tcp.port="$AKKA_REMOTING_BIND_PORT"""",
    "-Dakka.io.dns.resolver=async-dns",
    "-Dakka.io.dns.async-dns.resolve-srv=true",
    "-Dakka.io.dns.async-dns.resolv-conf=on"
 */
  javaOptions in Universal ++= Seq(
    // -J params will be added as jvm parameters
   "-J-Xmx12000m" //,
  //  "-J-Xms4000m",

    // you can access any build setting/task here
    //s"-version=${version.value}"
  )

)

lazy val coreDependencies = Seq(
  "com.github.pathikrit" %% "better-files" % "3.6.0",
  "com.roundeights" %% "hasher" % "1.2.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-remote" % versions.akka,
  "ch.megard" %% "akka-http-cors" % versions.akkaHttpCors,
  "de.heikoseeberger" %% "akka-http-json4s" % "1.16.1",
  "org.json4s" %% "json4s-native" % "3.6.1",
  "com.madgag.spongycastle" % "core" % versions.spongyCastle,
  "com.madgag.spongycastle" % "prov" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bcpg-jdk15on" % versions.spongyCastle,
  "com.madgag.spongycastle" % "bctls-jdk15on" % versions.spongyCastle,
  "org.bouncycastle" % "bcprov-jdk15on" % "1.51",
  "org.iq80.leveldb"            % "leveldb"          % "0.10" withSources() withJavadoc(),
  "com.codahale" % "shamir" % "0.6.0" withSources() withJavadoc(),
  "org.json4s" %% "json4s-ext" % "3.5.2",
  "org.scalaj" %% "scalaj-http" % "2.4.1" withJavadoc() withSources(),
  "com.twitter" %% "chill" % "0.9.3",
  "com.twitter" %% "algebird-core" % "0.13.4",
  "org.typelevel" %% "cats-core" % "1.3.1",
  "net.glxn" % "qrgen" % "1.4",
  "com.softwaremill.macmemo" %% "macros" % "0.4" withJavadoc() withSources(),
  "com.typesafe.slick" %% "slick" % "3.2.3",
  "com.h2database" % "h2" % "1.4.197",
  "com.twitter" %% "storehaus-cache" % "0.15.0"
  // "com.esotericsoftware" % "kryo" % "4.0.2"
)

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

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    Defaults.itSettings,
    libraryDependencies ++= (coreDependencies ++ testDependencies)
    // other settings here
  )
