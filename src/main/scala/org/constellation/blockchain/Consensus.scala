package org.constellation.blockchain

import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.rpc.ChainInterface.ResponseBlock

import scala.collection.mutable.ListBuffer
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ChainInterface

import scala.collection.mutable.HashMap

object Consensus {
  case class MineBlock( data: String )

  val chainCache = scala.collection.mutable.HashMap[String, Long]()

  /*
  We're prob going to want a buffering service to handle validation/updates to chainCache
   */
  def validateBuffer(buffer: ListBuffer[BlockData]) = buffer.filter(validData)

  def validData(tx: BlockData) = true
}


trait Consensus {
  this: ChainInterface with PeerToPeer with Receiver =>

  val buffer: ListBuffer[BlockData] = new ListBuffer[BlockData]()

  receiver {
    case transaction: BlockData =>
      buffer += transaction
      val peerMessage = ResponseBlock(blockChain.latestBlock)
      broadcast(peerMessage)
      sender() ! peerMessage

    case MineBlock(data) =>
      blockChain = blockChain.addBlock(data)
      val peerMessage = ResponseBlock(blockChain.latestBlock)
      broadcast(peerMessage)
      sender() ! peerMessage
  }
}
