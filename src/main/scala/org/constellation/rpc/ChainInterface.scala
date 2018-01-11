package org.constellation.rpc

import com.typesafe.scalalogging.Logger
import org.constellation.actor.Receiver
import org.constellation.blockchain.{Block, Chain}
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{GetId, Id}

import scala.util.{Failure, Success}


object ChainInterface {

  case object QueryLatest
  case object QueryAll

  case class ResponseBlockChain(blockChain: Chain)
  case class ResponseBlock(block: Block)

}

trait ChainInterface {
  this: PeerToPeer with Receiver =>

  import ChainInterface._

  val logger = Logger("PeerToPeerCommunication")

  var blockChain: Chain

  receiver {
    case QueryLatest => sender() ! responseLatest
    case QueryAll => sender() ! responseBlockChain

    case GetId => sender() ! Id(blockChain.id)

    //FIXME: This is inefficient
    case ResponseBlock(block) => handleBlockChainResponse(Seq(block))
    case ResponseBlockChain(blockChain) => handleBlockChainResponse(blockChain.blocks)
  }

  def handleBlockChainResponse( receivedBlocks: Seq[Block] ): Unit = {
    val localLatestBlock = blockChain.latestBlock
    logger.info(s"${receivedBlocks.length} blocks received.")

    receivedBlocks match {
      case Nil => logger.warn("Received an empty block list, discarding")

      case latestReceivedBlock :: _ if latestReceivedBlock.index <= localLatestBlock.index =>
        logger.debug("received blockchain is not longer than received blockchain. Do nothing")

      case latestReceivedBlock :: Nil if latestReceivedBlock.previousHash == localLatestBlock.hash =>
         logger.info("We can append the received block to our chain.")
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

  def responseBlockChain = ResponseBlockChain(blockChain)

}


