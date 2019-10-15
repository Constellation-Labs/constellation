import sbt.{Configuration, Defaults, Test, inConfig}
import sbt._
import sbt.Keys._

object E2E {
  final val E2ETest = Configuration.of("EndToEndTest", "e2e").extend(Test)

  final val e2eSettings = inConfig(E2ETest)(e2eConfig)

  lazy val e2eConfig =
    Defaults.configSettings ++ Defaults.testTasks ++ Seq(
      scalaSource in E2ETest := baseDirectory.value / "src" / "e2e" / "scala",
      javaSource in E2ETest := baseDirectory.value / "src" / "e2e" / "java",
      resourceDirectory in E2ETest := baseDirectory.value / "src" / "e2e" / "resources",
      E2ETest / parallelExecution := false
    )
}
