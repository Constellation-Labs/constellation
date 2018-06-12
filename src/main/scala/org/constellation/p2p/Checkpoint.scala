package org.constellation.p2p

import java.security.PublicKey

import akka.actor.ActorRef
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Schema._
import constellation._

trait Checkpoint extends PeerAuth {

  val data: Data
  import data._

  val consensusActor: ActorRef
  val publicKey: PublicKey

  def checkpointHeartbeat(): Unit = {

    if (!checkpointInProgress) {
      checkpointInProgress = true

      val memPoolSample = memPoolTX.take(20).toSeq

      // TODO: temporarily using all
      val facilitators = peerIDLookup.keys.toSet + Id(publicKey)

      val roundHash = RoundHash("test")

      val bundle = Bundle(BundleData(memPoolSample).signed())

      val vote = CheckpointVote(bundle)

      val callback = (bundle: ConsensusRoundResult[_ <: CC]) =>  {
        logger.debug(s"got checkpoint heartbeat callback = $bundle")
        checkpointInProgress = false
      }

      consensusActor ! InitializeConsensusRound(facilitators, roundHash, callback, vote)
    }

  }

}
