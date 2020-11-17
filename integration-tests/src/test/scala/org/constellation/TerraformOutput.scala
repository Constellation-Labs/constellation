package org.constellation

import org.scalatest.Assertions
import pureconfig.generic.ProductHint
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource, SnakeCase}
import pureconfig.generic.auto._

case class RootTerraformOutput(
  instanceIps: TerraformValue[List[String]]
)

case class TerraformValue[T](
  value: T
)

trait TerraformOutput extends Assertions {
  implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, SnakeCase))

  lazy val terraform: RootTerraformOutput = {
    val result = ConfigSource.resources("terraform-output.json").load[RootTerraformOutput]

    result match {
      case Right(value) => value
      case Left(value)  => cancel(value.prettyPrint())
    }
  }
}
