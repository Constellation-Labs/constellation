package org.constellation.actor

import akka.actor.Props
import com.typesafe.scalalogging.Logger
import org.constellation.blockchain.Consensus
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

object Node {
  def props(publicKey: String): Props = Props(new Node(publicKey))
}

class Node(override val publicKey: String) extends Receiver with PeerToPeer
  with ProtocolInterface with Consensus {
  override val logger = Logger("Node")
}





