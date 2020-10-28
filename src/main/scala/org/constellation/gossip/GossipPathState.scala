package org.constellation.gossip

import enumeratum.{Enum, EnumEntry}

sealed trait GossipPathState extends EnumEntry

object GossipPathState extends Enum[GossipPathState] {
  case object Pending extends GossipPathState
  case object Timeout extends GossipPathState
  case object Retrying extends GossipPathState

  val values = findValues
}
