enablePlugins(ScalaJSPlugin)

scalaVersion := "2.12.8" // or any other Scala version >= 2.10.2

//persistLauncher in Compile := true

//persistLauncher in Test := false

//skip in packageJSDependencies := false

// testFrameworks += new TestFramework("utest.runner.Framework")

libraryDependencies ++= Seq(
  "org.scala-js" %%% "scalajs-dom" % "0.9.6" withSources() withJavadoc(),
  "com.lihaoyi" %%% "scalarx" % "0.4.0" withSources() withJavadoc(),
  "com.lihaoyi" %%% "upickle" % "0.7.1",
  "com.lihaoyi" %%% "scalatags" % "0.6.7" withSources() withJavadoc(),
  "com.github.japgolly.scalacss" %%% "core" % "0.5.5" withSources() withJavadoc(),
  "com.github.japgolly.scalacss" %%% "ext-scalatags" % "0.5.5" withSources() withJavadoc(),
  "com.timushev" %%% "scalatags-rx" % "0.4.0"
  // "com.github.japgolly.scalajs-react" %%% "core" % "1.4.0"
  // "be.doeraene" %%% "scalajs-jquery" % "0.8.0"
)
