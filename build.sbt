import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

enablePlugins(JavaAppPackaging)

lazy val _version = "1.0.1"

lazy val versions = new {
  val akka = "2.4.18"
  val akkaHttp = "10.0.10"
  val akkaHttpCors = "0.2.2"
}


lazy val commonSettings = Seq(
  version := _version,
  scalaVersion := "2.12.2",
  organization := "org.constellation",
  name := "constellation",
  mainClass := Some("org.constellation.ConstellationNode"),
  parallelExecution in Test := false,
  dockerExposedPorts := Seq(2551, 9000, 16180, 6006, 9010),
  dockerCommands := dockerCommands.value.flatMap {
    case ExecCmd("ENTRYPOINT", args @ _*) => Seq(Cmd("ENTRYPOINT", args.mkString(" ")))
    case v => Seq(v)
  },
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
    """-DakkaActorSystemName="$AKKA_ACTOR_SYSTEM_NAME"""",
    """-Dakka.remote.netty.tcp.hostname="$(eval "echo $AKKA_REMOTING_BIND_HOST")"""",
    """-Dakka.remote.netty.tcp.port="$AKKA_REMOTING_BIND_PORT"""",
    "-Dakka.io.dns.resolver=async-dns",
    "-Dakka.io.dns.async-dns.resolve-srv=true",
    "-Dakka.io.dns.async-dns.resolv-conf=on"
  ),
  resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",

  javaOptions in Universal ++= Seq(
    // -J params will be added as jvm parameters
    "-J-Xmx12000m" //,
  //  "-J-Xms4000m",

    // you can access any build setting/task here
    //s"-version=${version.value}"
  )

)

lazy val coreDependencies = Seq(
  "com.roundeights" %% "hasher" % "1.2.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-remote" % versions.akka,
  "ch.megard" %% "akka-http-cors" % versions.akkaHttpCors,
  "de.heikoseeberger" %% "akka-http-json4s" % "1.16.1",
  "org.json4s" %% "json4s-native" % "3.5.2",
  "net.liftweb" %% "lift-json" % "3.1.1",
  "com.madgag.spongycastle" % "core" % "1.58.0.0",
  "com.madgag.spongycastle" % "prov" % "1.58.0.0",
  "com.madgag.spongycastle" % "bcpkix-jdk15on" % "1.58.0.0",
  "com.madgag.spongycastle" % "bcpg-jdk15on" % "1.58.0.0",
  "com.madgag.spongycastle" % "bctls-jdk15on" % "1.58.0.0",
  "net.liftweb" %% "lift-json" % "3.1.1",
  "com.google.guava" % "guava" % "21.0",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.51",
  "org.iq80.leveldb"            % "leveldb"          % "0.10" withSources() withJavadoc(),
  "org.fusesource.leveldbjni"   % "leveldbjni-all"   % "1.8" withSources() withJavadoc(),
  "com.codahale" % "shamir" % "0.6.0" withSources() withJavadoc(),
  "org.json4s" %% "json4s-ext" % "3.5.2",
  "org.scalaj" %% "scalaj-http" % "2.3.0" withJavadoc() withSources(),
  "com.twitter" %% "chill" % "0.9.1",
  "com.twitter" %% "algebird-core" % "0.13.4",
  "org.typelevel" %% "cats-core" % "1.0.1",
  "net.glxn" % "qrgen" % "1.4"
  // "com.esotericsoftware" % "kryo" % "4.0.2"
)

//Test dependencies
lazy val testDependencies = Seq(
  "org.scalacheck" %% "scalacheck" % "1.13.4",
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0",
  "com.typesafe.akka" %% "akka-http-testkit" % versions.akkaHttp,
  "com.typesafe.akka" %% "akka-testkit" % versions.akka
).map(_ % "it,test" )

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    Defaults.itSettings,
    libraryDependencies ++= (coreDependencies ++ testDependencies)
    // other settings here
  )
