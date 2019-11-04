package org.constellation.trust

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.consensus.StoredSnapshot
import org.constellation.schema.Id
import org.constellation.domain.trust.{TrustData, TrustDataInternal}
import org.constellation.primitives.concurrency.SingleRef

object TrustManager {

  def calculateReputation(snapshots: List[StoredSnapshot]): Map[Id, Double] =
    snapshots
      .flatMap(s => s.checkpointCache.flatMap(cpc => cpc.checkpointBlock.map(c => c.observations.toList)))
      .flatten
      .groupBy(o => o.signedObservationData.data.id)
      .mapValues(_.size.toDouble) // TODO: wkoszycki add conversion List[Observation] -> Score

  def calculateScoringMap(scores: List[TrustDataInternal]): Map[Id, Int] =
    scores.map(_.id).sortBy { _.hex }.zipWithIndex.toMap

  def calculateTrustNodes(
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
}

class TrustManager[F[_]: Concurrent](currentNodeId: Id) {

  /**
    * Reputation based on current node observations
    */
  private val storedReputation: SingleRef[F, Map[Id, Double]] = SingleRef(Map.empty)

  /**
    * Reputation based on current node observations with conjunction of cluster views
    */
  private val predictedReputation: SingleRef[F, Map[Id, Double]] = SingleRef(Map.empty)

  import TrustManager._

  def handleTrustScoreUpdate(peerTrustScores: List[TrustDataInternal]): F[Unit] =
    getRecentFinalSnapshots
      .map(snaps => peerTrustScores :+ TrustDataInternal(currentNodeId, TrustManager.calculateReputation(snaps)))
      .map { scores =>
        val scoringMap = calculateScoringMap(scores)
        val idxMap = scoringMap.map(_.swap)

        SelfAvoidingWalk
          .runWalkFeedbackUpdateSingleNode(
            scoringMap(currentNodeId),
            calculateTrustNodes(scores, currentNodeId, scoringMap)
          )
          .edges
          .map { e =>
            idxMap(e.dst) -> e.trust
          }
          .toMap
      }
      .flatMap(idMappedScores => predictedReputation.set(idMappedScores))

  def getRecentFinalSnapshots: F[List[StoredSnapshot]] = ??? // TODO: wkoszycki which Snapshots should be loaded ?

  def getPredictedReputation: F[Map[Id, Double]] = predictedReputation.getUnsafe

  def getStoredReputation: F[Map[Id, Double]] = storedReputation.getUnsafe

}
