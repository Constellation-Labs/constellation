package org.constellation.domain.configuration

import java.security.KeyPair

import org.constellation.ProcessingConfig
import org.constellation.keytool.KeyUtils
import org.constellation.schema.Id
import org.constellation.util.{AccountBalance, HostPort}

case class NodeConfig(
  seeds: Seq[HostPort] = Seq(),
  primaryKeyPair: KeyPair = KeyUtils.makeKeyPair(),
  isGenesisNode: Boolean = false,
  isLightNode: Boolean = false,
  isRollbackNode: Boolean = false,
  rollbackHeight: Long = 0L,
  rollbackHash: String = "",
  hostName: String = "127.0.0.1",
  httpInterface: String = "0.0.0.0",
  httpPort: Int = 9000,
  peerHttpPort: Int = 9001,
  defaultTimeoutSeconds: Int = 5,
  attemptDownload: Boolean = false,
  allowLocalhostPeers: Boolean = false,
  cliConfig: CliConfig = CliConfig(),
  processingConfig: ProcessingConfig = ProcessingConfig(),
  dataPollingManagerOn: Boolean = false,
  allocAccountBalances: Seq[AccountBalance] = Seq.empty,
  whitelisting: Map[Id, Option[String]] = Map.empty,
  minRequiredSpace: Int = 5
)
