resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

// addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.19")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M13-2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
// addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.11")
// addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")
// addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
// addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
//addSbtPlugin("com.artima.supersafe" %% "sbtplugin" % "1.1.7")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.lightbend.sbt" % "sbt-javaagent" % "0.1.5")
