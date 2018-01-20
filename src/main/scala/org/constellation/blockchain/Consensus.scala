package org.constellation.blockchain

import org.constellation.actor.Receiver
import org.constellation.rpc.ChainInterface.ResponseBlock
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ChainInterface

object Consensus {
  case class MineBlock( data: String )
}


trait Consensus {
  this: ChainInterface with PeerToPeer with Receiver =>

  receiver {
    case transaction: Transaction =>
      blockChain = blockChain.addBlock(transaction.message)
      val peerMessage = ResponseBlock(blockChain.latestBlock)
      broadcast(peerMessage)
      sender() ! peerMessage
  }
}
