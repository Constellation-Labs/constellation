package org.constellation.blockchain

import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.rpc.ProtocolInterface.ResponseBlock

import scala.collection.mutable.ListBuffer
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

import scala.collection.mutable

object Consensus {
  case class MineBlock( data: String )

  def validData(tx: BlockData) = true

  def validSignture(tx1: Tx, tx2: Tx) = true
}


trait Consensus {
  this: ProtocolInterface with PeerToPeer with Receiver =>

  val buffer: ListBuffer[BlockData] = new ListBuffer[BlockData]()
  val chainCache = mutable.HashMap[String, Long]()
  /*
  I'd like to consolidate these two, maybe make an object for each pub key, storing balance, reputation and half finished tx's
  Also, we should prob use some sort of db configurable based on tier.
   */
  val signatureCache = mutable.HashMap[String, Tx]()

  /*
  We're prob going to want a buffering service to handle validation/updates to chainCache
   */
  def updateCache(buffer: ListBuffer[Tx]) = {
    val validatedBuffer: mutable.Seq[Tx] = buffer.filter(Consensus.validData)

    validatedBuffer.foreach{tx: Tx =>
      signatureCache.get(tx.id) match {
        case Some(initialTx) if Consensus.validSignture(initialTx, tx) =>
          chainCache.update(tx.pubKey, chainCache(tx.id) + 1L)//TODO turn message: String into Long
        case None => signatureCache(tx.id) = tx
      }
    }
  }

  receiver {
    case transaction: BlockData =>
      buffer += transaction
      broadcast(transaction)
      sender() ! transaction

    case MineBlock(data) =>
      blockChain = blockChain.addBlock(data)
      val peerMessage = ResponseBlock(blockChain.latestBlock)
      broadcast(peerMessage)
      sender() ! peerMessage
  }
}
