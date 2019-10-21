package org.constellation.domain.configuration

import java.security.KeyPair

import org.constellation.crypto.KeyUtils
import org.constellation.util.{AccountBalance, HostPort}
import org.constellation.ProcessingConfig

case class NodeConfig(
  seeds: Seq[HostPort] = Seq(),
  primaryKeyPair: KeyPair = KeyUtils.makeKeyPair(),
  isGenesisNode: Boolean = false,
  isLightNode: Boolean = false,
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
  allocAccountBalances: Seq[AccountBalance] = Seq.empty
)
