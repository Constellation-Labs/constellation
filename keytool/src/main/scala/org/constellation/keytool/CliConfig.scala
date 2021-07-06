package org.constellation.keytool

import org.constellation.keytool.CliMethod.CliMethod

object CliMethod extends Enumeration {
  type CliMethod = Value

  val GenerateWallet, MigrateExistingKeyStoreToStorePassOnly, ExportPrivateKeyHex, PrintKeyInfo = Value
}

case class CliConfig(
  method: CliMethod = CliMethod.GenerateWallet,
  keystore: String = null,
  alias: String = null,
  storepass: Array[Char] = null,
  keypass: Array[Char] = null,
  loadFromEnvArgs: String = null
)
