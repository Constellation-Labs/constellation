package org.constellation.blockchain

import org.constellation.actor.Receiver
import org.constellation.blockchain.Consensus.MineBlock
import org.constellation.rpc.ProtocolInterface.ResponseBlock

import scala.collection.mutable.ListBuffer
import org.constellation.p2p.PeerToPeer
import org.constellation.rpc.ProtocolInterface

import scala.collection.mutable

object Consensus {
  case object MineBlock

  def validData(tx: BlockData) = true

  def validSignture(tx1: Tx, tx2: Tx) = true
}


trait Consensus {
  this: ProtocolInterface with PeerToPeer with Receiver =>

  val buffer: ListBuffer[BlockData] = new ListBuffer[BlockData]()
  val chainCache = mutable.HashMap[String, AccountData]()

  /*
  We're prob going to want a buffering service to handle validation/updates to chainCache
   */
  def updateCache(buffer: ListBuffer[Tx]) = {
    val validatedBuffer: mutable.Seq[Tx] = buffer.filter(Consensus.validData)

    validatedBuffer.foreach { tx: Tx =>
      val counterParty = chainCache.get(tx.counterPartyPubKey)
      val initialTransaction = counterParty.flatMap(_.unsignedTxs.get(tx.id))

      initialTransaction match {
        case Some(initialTx) if Consensus.validSignture(initialTx, tx) =>
          counterParty.foreach(acct => acct.unsignedTxs.remove(initialTx.id))
          chainCache.get(tx.senderPubKey).foreach(acct => acct.balance -= tx.amount)
          chainCache.get(tx.counterPartyPubKey).foreach(acct => acct.balance += tx.amount)
        case None =>
          counterParty.foreach(acct => acct.unsignedTxs.update(tx.id, tx))
      }
    }
  }

  receiver {
    case transaction: BlockData =>
      buffer += transaction
      broadcast(transaction)
      sender() ! transaction

    case MineBlock =>
      blockChain = blockChain.addBlock(buffer.mkString(" "))
      val peerMessage = ResponseBlock(blockChain.latestBlock)
      broadcast(peerMessage)
      sender() ! peerMessage
  }
}
