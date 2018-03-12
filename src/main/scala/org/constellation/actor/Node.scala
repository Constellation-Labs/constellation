package org.constellation.actor

import java.security.PublicKey

import akka.actor.Props
import com.typesafe.scalalogging.Logger
import org.constellation.blockchain.Consensus
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

object Node {
  def props(publicKey: PublicKey): Props = Props(new Node(publicKey))
}

class Node(override val publicKey: PublicKey) extends Receiver with PeerToPeer
  with ProtocolInterface with Consensus {
  override val logger = Logger("Node")
}