package org.constellation.rpc

import com.typesafe.scalalogging.Logger
import org.constellation.actor.Receiver
import org.constellation.blockchain._
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{GetBalance, GetId, Id}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}


object ProtocolInterface {

  case object QueryLatest
  case object QueryAll

  case class FullChain(blockChain: Chain)
  case class ResponseBlock(block: Block)
  case class Balance(balance: Long)

}

trait ProtocolInterface {
  this: PeerToPeer with Receiver =>

  val buffer: ListBuffer[BlockData] = new ListBuffer[BlockData]()
  val chainCache = mutable.HashMap[String, AccountData]()

  val chain = DAG

  import ProtocolInterface._

  val logger = Logger("PeerToPeerCommunication")

  var blockChain: Chain

  receiver {
    case QueryLatest => sender() ! responseLatest
    case QueryAll => sender() ! responseBlockChain

    case GetId => sender() ! Id(blockChain.id)

    case GetBalance(account) =>
      val test: Long = chainCache.get(account).map(_.balance).getOrElse(0L)
      logger.info(s"receiverd GetBalance request $test")
      sender() ! Balance(test)
    case CheckpointBlock =>
    //FIXME: This is inefficient
    case ResponseBlock(block) => instantiateChain(Seq(block))
    case FullChain(blockChain) => instantiateChain(blockChain.blocks)
  }

  def addBlock(block: Block): Unit = chain.globalChain.append(block)

  def instantiateChain(receivedBlocks: Seq[Block] ): Unit = {
    val localLatestBlock = blockChain.latestBlock
    logger.info(s"${receivedBlocks.length} blocks received.")

    receivedBlocks match {
      case Nil => logger.warn("Received an empty block list, discarding")

      case latestReceivedBlock :: _ if latestReceivedBlock.index <= localLatestBlock.index =>
        logger.debug("received blockchain is not longer than received blockchain. Do nothing")
//        if (latestReceivedBlock.recipient.contains(blockChain.id)) buffer.append(latestReceivedBlock)

      case latestReceivedBlock :: Nil if latestReceivedBlock.previousHash == localLatestBlock.hash =>
         logger.info("We can append the received block to our chain.")
//        if latestReceivedBlock.
        //TODO here, if block id = actor id add to sign buffer
            blockChain.addBlock(latestReceivedBlock) match {
              case Success(newChain) =>
                blockChain = newChain
                broadcast(responseLatest)
              case Failure(e) => logger.error("Refusing to add new block", e)
            }
      case _ :: Nil =>
            logger.info("We have to query the chain from our peer")
            broadcast(QueryAll)

      case _ =>
            logger.info("Received blockchain is longer than the current blockchain")
            Chain(blockChain.id, receivedBlocks) match {
              case Success(newChain) =>
                blockChain = newChain
                broadcast(responseBlockChain)
              case Failure(s) => logger.error("Rejecting received chain.", s)
            }
    }
  }

  def responseLatest = ResponseBlock(blockChain.latestBlock)

  def responseBlockChain = FullChain(blockChain)

}


