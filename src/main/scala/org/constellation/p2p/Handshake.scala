package org.constellation

import enumeratum.{CirceEnum, Enum, EnumEntry}

package object Handshake {

  sealed trait HandshakeStatus extends EnumEntry

  object HandshakeStatus extends Enum[HandshakeStatus] with CirceEnum[HandshakeStatus] {

    case object NoHandshake extends HandshakeStatus

    case object OneWayHandshake extends HandshakeStatus

    case object TwoWayHandshake extends HandshakeStatus

    val values = findValues
  }

}
