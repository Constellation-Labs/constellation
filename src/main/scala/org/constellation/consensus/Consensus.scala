package org.constellation.consensus

import java.security.{KeyPair, PrivateKey, PublicKey}

import akka.actor.{Actor, ActorRef}
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Block
import org.constellation.wallet.KeyUtils

import scala.collection.mutable

object Consensus {

  // Commands
  case class StartConsensusRound(previousBlock: Block)
  case class GetProposedBlock(dealerPublicKey: PublicKey)
  case class CheckConsensusResult()

  // Events
  case class ConsensusBlockCreated(consensusBlock: Block)
  case class ProposedBlockUpdated(block: Block)
  case class PeerProposedBlock(block: Block, peer: ActorRef)

  def getFacilitators(previousBlock: Block): Seq[ActorRef] = {
    // TODO: here is where we need to grab our random sampling fancy function

    previousBlock.clusterParticipants
  }

  def notifyFacilitatorsOfBlockProposal(proposedBlock: Block,
                                        self: ActorRef): Boolean = {
    val facilitators: Seq[ActorRef] = getFacilitators(proposedBlock)

    // make sure that we are a facilitator
    if (!isFacilitator(facilitators, self)) {
      return false
    }

    val facilitatorsToNotify = facilitators.filter(p => p != self)

    facilitatorsToNotify.foreach(f => {
      f ! PeerProposedBlock(proposedBlock, self)
    })

    // TODO: add signature round for each facilitator
    true
  }

  def isFacilitator(facilitators: Seq[ActorRef], self: ActorRef): Boolean = {
    facilitators.contains(self)
  }

  // TODO: is this only on transaction or blocks? both? a single block? or multiple blocks?
  def getConsensusBlock(peerBlockProposals: mutable.HashMap[ActorRef, Block],
                        currentFacilitators: Seq[ActorRef]): Option[Block] = {

    // if (some number of current facilitators above some threshold agree on a block then use that block

    None
  }

  case class ConsensusRoundState(proposedBlock: Option[Block] = None,
                                 previousBlock: Option[Block] = None,
                                 currentFacilitators: Seq[ActorRef] = Seq(),
                                 peerBlockProposals: mutable.HashMap[ActorRef, Block] = mutable.HashMap())

}

class Consensus(memPoolManager: ActorRef, chainManager: ActorRef, keyPair: KeyPair) extends Actor {

  var consensusRoundState: ConsensusRoundState = ConsensusRoundState()

  override def receive: Receive = {
    case StartConsensusRound(prevBlock) =>

      consensusRoundState = ConsensusRoundState(None,
        Some(prevBlock), Consensus.getFacilitators(prevBlock), mutable.HashMap())

      // If we are a facilitator this round then begin consensus
      if (Consensus.isFacilitator(consensusRoundState.currentFacilitators, self)) {
        // TODO: here we need to replace to get public key from PTKE distributed dealer
        val dealerPublicKey = KeyUtils.bytesToPublicKey("23lkjdsf".getBytes())

        memPoolManager ! GetProposedBlock(dealerPublicKey)
      }

    case ProposedBlockUpdated(block) =>

      consensusRoundState = consensusRoundState.copy(proposedBlock = Some(block))

      // Start the ACS process once we have our new proposed block
      // TODO: cleanup

      if (consensusRoundState.proposedBlock.isDefined && consensusRoundState.previousBlock.isDefined) {

        Consensus.notifyFacilitatorsOfBlockProposal(consensusRoundState.proposedBlock.get, self)

      }

    case PeerProposedBlock(block, peer) =>
      val updatedPeerBlockProposals = consensusRoundState.peerBlockProposals += (peer -> block)

      consensusRoundState = consensusRoundState.copy(peerBlockProposals = updatedPeerBlockProposals)

      self ! CheckConsensusResult

    case CheckConsensusResult =>
      val consensusBlock =
        Consensus.getConsensusBlock(consensusRoundState.peerBlockProposals, consensusRoundState.currentFacilitators)

      if (consensusBlock.isDefined) {
        chainManager ! ConsensusBlockCreated(consensusBlock.get)
      }
  }

}