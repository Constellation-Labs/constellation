package org.constellation.trust

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.observation.{Observation, ObservationEvent}
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.NodeState
import org.constellation.schema.Id

class TrustManager[F[_]](nodeId: Id, cluster: Cluster[F])(implicit F: Concurrent[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private val storedReputation: Ref[F, Map[Id, Double]] = Ref.unsafe(Map.empty)

  private val predictedReputation: Ref[F, Map[Id, Double]] = Ref.unsafe(Map.empty)

  def handleTrustScoreUpdate(peerTrustScores: List[TrustDataInternal]): F[Unit] =
    cluster.getPeerInfo.flatMap { peers =>
      if (peers.nonEmpty) {
        for {
          reputation <- getStoredReputation
          scores = peerTrustScores :+ TrustDataInternal(nodeId, reputation)
          scoringMap = calculateScoringMap(scores)
          idxMap = scoringMap.map(_.swap)

          idMappedScores = SelfAvoidingWalk
            .runWalkFeedbackUpdateSingleNode(scoringMap(nodeId), calculateTrustNodes(scores, nodeId, scoringMap))
            .edges
            .map(e => idxMap(e.dst) -> e.trust)
            .toMap

          _ <- storedReputation.modify(_ => (idMappedScores, ()))
          _ <- predictedReputation.modify(_ => (idMappedScores, ()))
        } yield ()
      } else logger.debug("Skipping trust score update")
    }

  def getPredictedReputation: F[Map[Id, Double]] = predictedReputation.get

  def getStoredReputation: F[Map[Id, Double]] = {

    def filterFn(peers: Map[Id, PeerData])(id: Id): Boolean =
      !peers.get(id).exists(p => NodeState.offlineStates.contains(p.peerMetadata.nodeState))

    cluster.getPeerInfo.flatMap { peers =>
      storedReputation.get.map(
        reputation => peers.mapValues(_ => 1d) + (nodeId -> 1d) ++ reputation.filterKeys(filterFn(peers))
      )
    }
  }

  def updateStoredReputation(o: Observation): F[Unit] = {
    val score = observationScoring(o.signedObservationData.data.event)
    val id = o.signedObservationData.data.id

    storedReputation.modify { reputation =>
      val updated = Math.max(reputation.getOrElse(id, 1d) + score, -1d)
      (reputation + (id -> updated), ())
    }
  }

//  private def calculateReputation(snapshots: List[StoredSnapshot]): Map[Id, Double] =
//    snapshots
//      .flatMap(_.checkpointCache.flatMap(_.checkpointBlock.flatMap(_.observations.toList)))
//      .groupBy(_.signedObservationData.data.id)
//      .mapValues(_.size.toDouble) // TODO: wkoszycki add conversion List[Observation] -> Score

  private def calculateScoringMap(scores: List[TrustDataInternal]): Map[Id, Int] =
    scores.map(_.id).sortBy { _.hex }.zipWithIndex.toMap

  private def calculateTrustNodes(
    scores: List[TrustDataInternal],
    currentNodeId: Id,
    scoringMap: Map[Id, Int]
  ): List[TrustNode] =
    scores.map {
      case TrustDataInternal(id, peerScores) =>
        println(s"${Console.RED}Size: ${peerScores.size}${Console.RESET}")
        val selfIdx = scoringMap(id)
        TrustNode(
          selfIdx,
          0d,
          0d,
          peerScores.map {
            case (peerId, score) =>
              println(s"${Console.RED}${score}${Console.RESET}")
              TrustEdge(selfIdx, scoringMap(peerId), score, id == currentNodeId)
          }.toSeq
        )
    }

  def observationScoring(event: ObservationEvent): Double = {
    import org.constellation.domain.observation._

    event match {
      case _: CheckpointBlockWithMissingParents => -0.1
      case _: CheckpointBlockWithMissingSoe     => -0.1
      case _: RequestTimeoutOnConsensus         => -0.1
      case _: RequestTimeoutOnResolving         => -0.1
      case _: SnapshotMisalignment              => -0.1
      case _: CheckpointBlockInvalid            => -0.1
      case _                                    => 0d
    }
  }
}

object TrustManager {
  def apply[F[_]: Concurrent](nodeId: Id, cluster: Cluster[F]): TrustManager[F] = new TrustManager[F](nodeId, cluster)
}
