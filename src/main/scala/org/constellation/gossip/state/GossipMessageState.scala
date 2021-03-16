package org.constellation.gossip.state

import enumeratum.{Enum, EnumEntry}

sealed trait GossipMessageState extends EnumEntry

object GossipMessageState extends Enum[GossipMessageState] {

  case object Pending extends GossipMessageState

  case object Success extends GossipMessageState

  val values = findValues
}
