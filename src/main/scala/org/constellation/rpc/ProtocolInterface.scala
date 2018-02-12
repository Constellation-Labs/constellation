package org.constellation.rpc

import com.typesafe.scalalogging.Logger
import org.constellation.actor.Receiver
import org.constellation.blockchain._
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{GetBalance, GetId, Id}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ProtocolInterface {

  case class GetLatestBlock()

  case class GetChain()

  case class FullChain(blockChain: Seq[CheckpointBlock])

  case class ResponseBlock(block: CheckpointBlock)

  case class Balance(balance: Long)

  /**
    * Stubbed, this will use the checkpoint block and validSignature
    *
    * @param tx
    * @return
    */
  def validBlockData(tx: BlockData): Boolean = true

  def validCheckpointBlock(cp: CheckpointBlock): Boolean = true

  /**
    * This will require some logic in the DAG object, which passes the block and proposed delegate list to the generating
    * function (coalgebra class)
    *
    * @return Boolean is this currently Node a delegate
    */
  def isDelegate: Boolean = true

  def validSignature(tx1: Tx, tx2: Tx): Boolean = true
}

trait ProtocolInterface {
  this: PeerToPeer with Receiver =>

  import ProtocolInterface._

  val logger = Logger("ProtocolInterface")

  val buffer: ListBuffer[BlockData] = new ListBuffer[BlockData]()
  val chainCache = mutable.HashMap[String, AccountData]()
  val signatureBuffer = new ListBuffer[BlockData]()

  val chain = DAG
  val publicKey: String

  receiver {
    case transaction: BlockData =>
      logger.info(s"received transaction from ${sender()}")
      buffer += transaction
      broadcast(transaction)
      sender() ! transaction

    case GetLatestBlock =>
      logger.info(s"received GetLatestBlock request from ${sender()}")
      sender() ! chain.globalChain.head

    case GetChain =>
      logger.info(s"received GetChain request from ${sender()}")
      sender() ! chain.globalChain

    case GetId =>
      logger.info(s"received GetId request ${sender()}")
      sender() ! Id(publicKey)

    case GetBalance(account) =>
      val balance = chainCache.get(account).map(_.balance).getOrElse(0L)
      logger.info(s"received GetBalance request, balance is: $balance")
      sender() ! Balance(balance)

    case checkpointBlock: CheckpointBlock =>
      logger.info(s"received CheckpointBlock request ${sender()}")
      if (validCheckpointBlock(checkpointBlock))
        addBlock(checkpointBlock)

    case fullChain: FullChain =>
      val newBlocks = fullChain.blockChain.diff(chain.globalChain)
      logger.info(s"received fullChain from ${sender()} \n diff is $newBlocks")
      if (newBlocks.nonEmpty && newBlocks.forall(validCheckpointBlock))
        newBlocks.foreach(addBlock)
  }

  /**
    * Append block to the globalChain
    *
    * @param block CheckpointBlock to be added
    * @return Unit
    */
  def addBlock(block: CheckpointBlock) = {
    logger.info(s"block $block Added")
    chain.globalChain.append(block)
  }

  /**
    * We're prob going to want a buffering service to handle validation/updates to chain cache, this is for maintaining
    * balances on the ledger
    *
    * @param buffer ListBuffer[Tx] all Tx's within the buffer that will now be used to update chain state
    */
  def updateLedgerState(buffer: ListBuffer[Tx]): Unit = {
    val validatedBuffer: mutable.Seq[Tx] = buffer.filter(validBlockData)

    validatedBuffer.foreach { tx: Tx =>
      val counterParty = chainCache.get(tx.counterPartyPubKey)
      val initialTransaction = counterParty.flatMap(_.unsignedTxs.get(tx.id))

      initialTransaction match {
        case Some(initialTx) if validSignature(initialTx, tx) =>
          counterParty.foreach(acct => acct.unsignedTxs.remove(initialTx.id))
          chainCache.get(tx.senderPubKey).foreach(acct => acct.balance -= tx.amount)
          chainCache.get(tx.counterPartyPubKey).foreach(acct => acct.balance += tx.amount)
        case None =>
          counterParty.foreach(acct => acct.unsignedTxs.update(tx.id, tx))
      }
    }
  }
}


