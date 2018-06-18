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

    if (!downloadMode) {

      var roundHash: RoundHash[_ <: CC] = RoundHash(genesisTXHash)

      if (lastCheckpointBundle.isDefined) {
        roundHash = RoundHash(lastCheckpointBundle.get.roundHash)
      }

      if (checkpointsInProgress.get(roundHash).isEmpty) {

        checkpointsInProgress.putIfAbsent(roundHash, true)

        val memPoolSample = memPoolTX.toSeq

        // TODO: temporarily using all
        val facilitators = peerIDLookup.keys.toSet + Id(publicKey)

        val bundle = Bundle(BundleData(memPoolSample).signed())

        val vote = CheckpointVote(bundle)

        val callback = (result: ConsensusRoundResult[_ <: CC]) =>  {
          logger.debug(s"$publicKey got checkpoint heartbeat callback = $result")

          if (result.bundle.bundleData.data.bundles.nonEmpty) {
            bundles = bundles + result.bundle
          }

          logger.debug(s"bundles = $bundles")

          lastCheckpointBundle = Some(result.bundle)

          // cleanup mem pool
          lastCheckpointBundle.toIterator.foreach(f => {
            val txs: Set[TX] = f.extractTX
            memPoolTX --= txs.toList
            validTX ++= txs
          })

          checkpointsInProgress.putIfAbsent(roundHash, false)
          ()
        }

        consensusActor ! InitializeConsensusRound(facilitators, roundHash, callback, vote)
      }

    }

  }

}
