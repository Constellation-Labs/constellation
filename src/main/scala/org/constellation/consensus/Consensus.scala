package org.constellation.consensus

import java.security.{KeyPair, PrivateKey, PublicKey}

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.constellation.consensus.Consensus._
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.ChainStateManager.{AddBlock, CreateBlockProposal}
import org.constellation.wallet.KeyUtils

import scala.collection.mutable

/*

  Consensus Flow (temporary):

  - a consensus round starts

  - we grab the latest set of transactions up to some limit

  - we send each "facilitator" every set of transactions

  - on the node we keep track of a map of facilitator ref to set of transactions

  - we take a union of the transactions

  - we create a block

  - we send the block to each of the nodes again

  - we keep track of the map of facilitator to blocks

  - once we have all of them, we compare the blocks and if they are all the same then we add the block to our local chain

  - we remove the transactions in the block from our mem pool

  - start the process again

  - if they are not the same then we reset the state and start the process again

 */
object Consensus {

  // Commands
  case class StartConsensusRound(previousBlock: Block)
  case class GetMemPool(dealerPublicKey: PublicKey)
  case class CheckConsensusResult()

  // Events
  case class ProposedBlockUpdated(block: Block)
  case class MemPoolUpdated(transactions: Seq[Transaction])
  case class PeerMemPoolUpdated(transactions: Seq[Transaction], peer: ActorRef)
  case class PeerProposedBlock(block: Block, peer: ActorRef)

  def getFacilitators(previousBlock: Block): Seq[ActorRef] = {
    // TODO: here is where we need to grab our random sampling fancy function

    previousBlock.clusterParticipants
  }

  def isFacilitator(facilitators: Seq[ActorRef], self: ActorRef): Boolean = {
    facilitators.contains(self)
  }

  def getConsensusBlock(peerBlockProposals: mutable.HashMap[ActorRef, Block],
                        currentFacilitators: Seq[ActorRef]): Option[Block] = {

    val facilitatorsWithoutBlockProposals = currentFacilitators.filterNot(f => {
      peerBlockProposals.contains(f)
    })

    var consensusBlock: Option[Block] = None

    if (facilitatorsWithoutBlockProposals.isEmpty) {

      val blocks = peerBlockProposals.values

      // TODO: update to be from a threshold not all
      val allBlocksInConsensus = blocks.toList.distinct.length == 1

      if (allBlocksInConsensus) {
        consensusBlock = Some(blocks.head)
      }
    }

    consensusBlock
  }

  def notifyFacilitators(previousBlock: Block, self: ActorRef, fx: ActorRef => Unit): Boolean = {
    val facilitators: Seq[ActorRef] = getFacilitators(previousBlock)

    // make sure that we are a facilitator
    if (!isFacilitator(facilitators, self)) {
      return false
    }

    facilitators.foreach(fx)

    true
  }

  def notifyFacilitatorsOfBlockProposal(previousBlock: Block, proposedBlock: Block, self: ActorRef): Boolean = {
    notifyFacilitators(previousBlock, self, (f) => f ! PeerProposedBlock(proposedBlock, self))
  }

  def notifyFacilitatorsOfPeerMemPoolUpdated(previousBlock: Block, self: ActorRef, transactions: Seq[Transaction]): Boolean = {
    // Send all of the facilitators our current memPoolState
    Consensus.notifyFacilitators(previousBlock, self, (p) => {
      p ! PeerMemPoolUpdated(transactions, self)
    })
  }

  case class ConsensusRoundState(proposedBlock: Option[Block] = None,
                                 previousBlock: Option[Block] = None,
                                 currentFacilitators: Seq[ActorRef] = Seq(),
                                 peerMemPools: mutable.HashMap[ActorRef, Seq[Transaction]] = mutable.HashMap(),
                                 peerBlockProposals: mutable.HashMap[ActorRef, Block] = mutable.HashMap())

}

class Consensus(memPoolManager: ActorRef, chainManager: ActorRef, keyPair: KeyPair) extends Actor with ActorLogging {

  var consensusRoundState: ConsensusRoundState = ConsensusRoundState()

  override def receive: Receive = {
    case StartConsensusRound(prevBlock) =>

      consensusRoundState = ConsensusRoundState(None,
        Some(prevBlock), Consensus.getFacilitators(prevBlock), mutable.HashMap(), mutable.HashMap())

      // If we are a facilitator this round then begin consensus
      if (Consensus.isFacilitator(consensusRoundState.currentFacilitators, self)) {
        // TODO: here we need to replace to get public key from PTKE distributed dealer
        val dealerPublicKey = KeyUtils.bytesToPublicKey("23lkjdsf".getBytes())

        memPoolManager ! GetMemPool(dealerPublicKey)
      }

    case ProposedBlockUpdated(block) =>

      consensusRoundState = consensusRoundState.copy(proposedBlock = Some(block))

      val previousBlock = consensusRoundState.previousBlock
      val proposedBlock = consensusRoundState.proposedBlock

      if (proposedBlock.isDefined && previousBlock.isDefined) {
        Consensus.notifyFacilitatorsOfBlockProposal(previousBlock.get, proposedBlock.get, self)
      }

    case MemPoolUpdated(transactions) =>
      Consensus.notifyFacilitatorsOfPeerMemPoolUpdated(consensusRoundState.previousBlock.get, self, transactions)

    case PeerMemPoolUpdated(transactions, peer) =>

      // TODO: extract
      consensusRoundState.peerMemPools.put(peer, transactions)

      // check if we have enough mem pools to create a block

      val facilitatorsWithoutMemPools = consensusRoundState.currentFacilitators.filterNot(f => {
        consensusRoundState.peerMemPools.contains(f)
      })

      if (facilitatorsWithoutMemPools.nonEmpty) {
        log.debug("Have not received enough mem pools to create a block yet")
      } else {
        chainManager ! CreateBlockProposal(consensusRoundState.peerMemPools)
      }

    case PeerProposedBlock(block, peer) =>
      val updatedPeerBlockProposals = consensusRoundState.peerBlockProposals += (peer -> block)

      consensusRoundState = consensusRoundState.copy(peerBlockProposals = updatedPeerBlockProposals)

      self ! CheckConsensusResult

    case CheckConsensusResult =>
      val consensusBlock =
        Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals, consensusRoundState.currentFacilitators)

      if (consensusBlock.isDefined) {
        chainManager ! AddBlock(consensusBlock.get)
      }
  }

}