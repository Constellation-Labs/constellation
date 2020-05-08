package org.constellation.wallet

import org.constellation.wallet.CliMethod.CliMethod

object CliMethod extends Enumeration {
  type CliMethod = Value

  val ShowAddress, ShowId, ShowPublicKey, CreateTransaction = Value

}

case class CliConfig(
  method: CliMethod = null,
  keystore: String = null,
  alias: String = null,
  storepass: Array[Char] = null,
  keypass: Array[Char] = null,
  loadFromEnvArgs: String = null,
  destination: String = null,
  prevTxPath: String = null,
  txPath: String = null,
  fee: Double = 0,
  amount: Long = 0L,
  normalized: Boolean = false
)
