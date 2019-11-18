package org.constellation.trust

import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.observation.{Observation, ObservationEvent}
import org.constellation.schema.Id
import org.constellation.domain.trust.TrustDataInternal

class TrustManager[F[_]: Concurrent](nodeId: Id) {

  private val storedReputation: Ref[F, Map[Id, Double]] = Ref.unsafe(Map.empty)

  private val predictedReputation: Ref[F, Map[Id, Double]] = Ref.unsafe(Map.empty)

  def handleTrustScoreUpdate(peerTrustScores: List[TrustDataInternal]): F[Unit] =
    for {
      reputation <- storedReputation.get
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

  def getPredictedReputation: F[Map[Id, Double]] = predictedReputation.get

  def getStoredReputation: F[Map[Id, Double]] = storedReputation.get

  def updateStoredReputation(o: Observation): F[Unit] = {
    val score = observationScoring(o.signedObservationData.data.event)
    val id = o.signedObservationData.data.id

    storedReputation.modify { reputation =>
      val updated = Math.max(reputation.getOrElse(id, 0d) - score, -1d)
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
        val selfIdx = scoringMap(id)
        TrustNode(selfIdx, 0d, 0d, peerScores.map {
          case (peerId, score) =>
            TrustEdge(selfIdx, scoringMap(peerId), score, id == currentNodeId)
        }.toSeq)
    }

  private def observationScoring(event: ObservationEvent): Double = {
    import org.constellation.domain.observation._

    event match {
      case _: CheckpointBlockWithMissingParents => 0.1
      case _: CheckpointBlockWithMissingSoe     => 0.1
      case _: RequestTimeoutOnConsensus         => 0.1
      case _: RequestTimeoutOnResolving         => 0.1
      case _: SnapshotMisalignment              => 0.1
      case _: CheckpointBlockInvalid            => 0.1
      case _                                    => 0d
    }
  }
}

object TrustManager {
  def apply[F[_]: Concurrent](nodeId: Id): TrustManager[F] = new TrustManager[F](nodeId)
}
