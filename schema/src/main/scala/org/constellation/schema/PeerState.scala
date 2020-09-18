package org.constellation.schema

import enumeratum.{CirceEnum, Enum, EnumEntry}

sealed trait PeerState extends EnumEntry

object PeerState extends Enum[PeerState] with CirceEnum[PeerState] {
  case object Join extends PeerState
  case object Leave extends PeerState

  val values = findValues
}
