import sbt.Keys._
import sbt.{Configuration, Defaults, Test, inConfig, _}

object Regression {
  final val RegressionTest = Configuration.of("RegressionTest", "regression").extend(Test)

  final val regressionSettings = inConfig(RegressionTest)(regressionConfig)

  lazy val regressionConfig =
    Defaults.configSettings ++ Defaults.testTasks ++ Seq(
      scalaSource in RegressionTest := baseDirectory.value / "src" / "regression" / "scala",
      javaSource in RegressionTest := baseDirectory.value / "src" / "regression" / "java",
      resourceDirectory in RegressionTest := baseDirectory.value / "src" / "regression" / "resources",
      RegressionTest / parallelExecution := false
    )
}
