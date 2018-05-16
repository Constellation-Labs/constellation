package org.constellation.state

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.scalalogging.Logger
import org.constellation.LevelDB
import org.constellation.consensus.Consensus.{PeerProposedBlock, ProposedBlockUpdated, RequestBlockProposal}
import org.constellation.p2p.PeerToPeer.Id
import org.constellation.p2p.{UDPSendToID, UDPSendToIDByte}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.Schema.{GetUTXO, TX}
import org.constellation.state.ChainStateManager._
import org.constellation.state.MemPoolManager.RemoveConfirmedTransactions

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.util.{Failure, Try}

object ChainStateManager {

  // Commands
  case class AddBlock(block: Block, replyTo: ActorRef)
  case class GetCurrentChainState()
  case class CreateBlockProposal(memPools: HashMap[Id, Seq[Transaction]], round: Long, replyTo: ActorRef)

  // Events
  case class BlockAddedToChain(previousBlock: Block)
  case class CurrentChainStateUpdated(chain: Chain)

  def handleAddBlock(chain: Chain, block: Block, memPoolManager: ActorRef, replyTo: ActorRef): Chain = {
    var updatedChain = chain

    if (!updatedChain.chain.contains(block)) {
      updatedChain = Chain(updatedChain.chain :+ block)
      memPoolManager ! RemoveConfirmedTransactions(block.transactions)
      replyTo ! BlockAddedToChain(block)
    }

    updatedChain
  }

  def handleCreateBlockProposal(memPools: Map[Id, Seq[Transaction]], chain: Chain, round: Long, replyTo: ActorRef): Block = {
    val transactions: Seq[Transaction] = memPools.foldLeft(Seq[Transaction]()) {
      (result, b) => {
        result.union(b._2).distinct.sortBy(t => t.sequenceNum)
      }
    }

    val lastBlock = chain.chain.last

    val round = lastBlock.round + 1

    // TODO: update to use proper sigs and participants
    val blockProposal: Block =  Block(lastBlock.signature, lastBlock.height + 1, "",
      lastBlock.clusterParticipants, round, transactions)

    replyTo ! ProposedBlockUpdated(blockProposal)
    blockProposal
  }

  case object GetChain

}

class ChainStateManager(memPoolManagerActor: ActorRef, selfId: Id = null, db: LevelDB = null) extends Actor with ActorLogging {

  @volatile var chain: Chain = Chain()
  val logger = Logger(s"ChainStateManager")
  @volatile var lastBlockProposed: Option[Block] = None

  private val UTXO = mutable.HashMap[String, Long]()

  // This should be identical to levelDB hashes but I'm putting here as a way to double check
  // Ideally the hash workload should prioritize memory and dump to disk later but can be revisited.
  private val addressToTX = mutable.HashMap[String, TX]()

  def processTransaction(tx: TX): Unit = {

    // logger.debug(s"Processing TX: ${tx.short} on ${id.medium}")
    db.put(tx)

    val txDat = tx.tx.data

    // logger.debug(s"UTXO Before TX ${tx.short} $UTXO")

    if (txDat.isGenesis) {
      // UTXO(txDat.src.head.address) = txDat.inverseAmount
      // Move elsewhere ^ too complex.
      UTXO(txDat.dst.address) = txDat.amount
    } else {

      val total = txDat.src.map{s => UTXO(s.address)}.sum
      val remainder = total - txDat.amount

      // Temporarily disable source address to reduce complexity -- re-enable later.
      txDat.src.foreach{ s => UTXO.remove(s.address) }

      val prevDstBalance = UTXO.getOrElse(txDat.dst.address, 0L)
      UTXO(txDat.dst.address) = prevDstBalance + txDat.amount

      txDat.remainder.foreach{ r =>
        val prv = UTXO.getOrElse(r.address, 0L)
        UTXO(r.address) = prv + remainder
      }
    }
    //   logger.debug(s"UTXO After TX ${tx.short} $UTXO")

    txDat.src.foreach{ s =>
      addressToTX(s.address) = tx
      db.put(s.address, tx)
    }

    db.put(txDat.dst.address, tx)
    addressToTX(txDat.dst.address) = tx

  }

  override def receive: Receive = {

    case tx: TX =>

      if (!db.contains(tx)) processTransaction(tx)

    case GetUTXO =>
      val map = UTXO.toMap
      sender() ! map

    case GetChain => sender() ! chain

    case AddBlock(block, replyTo) =>
      chain = handleAddBlock(chain, block, memPoolManagerActor, replyTo)

    case GetCurrentChainState =>
      sender() ! CurrentChainStateUpdated(chain)

    case CreateBlockProposal(memPools, round, replyTo) =>
      lastBlockProposed = Some(handleCreateBlockProposal(memPools, chain, round, replyTo))

  }

}

