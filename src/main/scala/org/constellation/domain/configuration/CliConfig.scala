package org.constellation.domain.configuration

// scopt requires default args for all properties.
// Make sure to check for null early -- don't propogate nulls anywhere else.
case class CliConfig(
  externalIp: java.net.InetAddress = null,
  externalPort: Int = 0,
  allocFilePath: String = null,
  debug: Boolean = false,
  startOfflineMode: Boolean = false,
  lightNode: Boolean = false,
  genesisNode: Boolean = false,
  testMode: Boolean = false
)
