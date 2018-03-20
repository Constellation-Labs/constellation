package org.constellation.actor

import com.typesafe.scalalogging.Logger
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface


class Protocol(override val publicKey: String) extends Receiver with PeerToPeer
  with ProtocolInterface {
  override val logger = Logger("Node")
}