package org.constellation

import org.scalatest.Assertions
import pureconfig.ConfigSource
import pureconfig.generic.auto._

case class KeyPairConfig(
  keystore: String,
  keypass: String,
  alias: String,
  storepass: String
)

case class TxGenConfig(
  dst: String,
  amount: Long,
  fee: Long,
  countTotal: Int,
  countPerHost: Int,
  keyPair: KeyPairConfig
)

case class RootTestConfig(
  txGen: TxGenConfig
)

trait TestConfig extends Assertions {

  lazy val config: RootTestConfig = {
    val result = ConfigSource.resources("test.conf").load[RootTestConfig]

    result match {
      case Right(value) => value
      case Left(value)  => cancel(value.prettyPrint())
    }
  }

}
