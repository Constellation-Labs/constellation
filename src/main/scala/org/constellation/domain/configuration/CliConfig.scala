package org.constellation.domain.configuration

import org.constellation.schema.NodeType
import org.constellation.schema.NodeType.Light

// scopt requires default args for all properties.
// Make sure to check for null early -- don't propogate nulls anywhere else.
case class CliConfig(
  externalIp: java.net.InetAddress = null,
  externalPort: Int = 0,
  allocFilePath: String = null,
  allocFileNormalized: Boolean = false,
  keyStorePath: String = null,
  storePassword: String = null,
  keyPassword: String = null,
  alias: String = null,
  whitelisting: String = null,
  debug: Boolean = false,
  startOfflineMode: Boolean = false,
  nodeType: NodeType = Light,
  genesisNode: Boolean = false,
  rollbackNode: Boolean = false,
  rollbackHeight: Long = 0L,
  rollbackHash: String = null,
  testMode: Boolean = false,
  cloud: String = null
)
