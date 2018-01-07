package org.constellation.actor

import akka.actor.Props
import com.typesafe.scalalogging.Logger
import org.constellation.blockchain.{Chain, Consensus}
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ChainInterface

object ChainActor {
  def props( blockChain: Chain ): Props = Props(new ChainActor(blockChain))
}

class ChainActor(var blockChain: Chain ) extends Receiver with PeerToPeer
  with ChainInterface with Consensus {
  override val logger = Logger("BlockChainActor")
}





