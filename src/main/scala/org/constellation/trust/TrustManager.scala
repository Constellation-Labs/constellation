package org.constellation.trust

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.observation._
import org.constellation.schema.{Id, NodeState}

class TrustManager[F[_]](nodeId: Id, clusterStorage: ClusterStorageAlgebra[F])(implicit F: Concurrent[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private val storedReputation: Ref[F, Map[Id, Double]] = Ref.unsafe(Map.empty)

  private val predictedReputation: Ref[F, Map[Id, Double]] = Ref.unsafe(Map.empty)

  def getTrustDataInternalSelf: F[TrustDataInternal] = getStoredReputation.map(TrustDataInternal(nodeId, _))

  def handleTrustScoreUpdate(peerTrustScores: List[TrustDataInternal]): F[Unit] =
    clusterStorage.getPeers.flatMap { peers =>
      if (peers.size > 2) {
        for {
          reputation <- getStoredReputation
          _ <- logger.info(s"Begin handleTrustScoreUpdate for peerTrustScores: ${peerTrustScores.toString()}")
          scores = peerTrustScores :+ TrustDataInternal(nodeId, reputation)
          (scoringMap, idxMap) = TrustManager.calculateIdxMaps(scores)
          //          _ <- logger.debug(s"Begin handleTrustScoreUpdate for scores: ${scores.toString()}")
//          _ <- logger.info(
//            s"Begin handleTrustScoreUpdate for distinct allNodeIds: ${scores.flatMap(_.view.keySet).distinct}"
//          )
//          _ <- logger.debug(s"Begin handleTrustScoreUpdate for peers: ${peers.map(_._1.address)}")
//          _ <- logger.debug(s"TrustManager.calculateIdxMaps for idxMap: ${idxMap.toString()}")
//          _ <- logger.debug(s"TrustManager.scoringMap for scoringMap: ${scoringMap.toString()}")
          selfNodeId = scoringMap(nodeId)

          missingNodes = scoringMap.keySet.toList.diff(scores.map(_.id))
          simulatedTrustDataForMissingNodes = missingNodes.map { id =>
            TrustDataInternal(id, scoringMap.mapValues(_ => 1d))
          }

          trustNodesInternal = scores ++ simulatedTrustDataForMissingNodes
          trustNodes = TrustManager.calculateTrustNodes(trustNodesInternal.distinct, nodeId, scoringMap)
          //          _ <- logger.debug(s"TrustManager.calculateTrustNodes for trustNodes: ${trustNodes.toString()}")

          idMappedScores: Map[Id, Double] = SelfAvoidingWalk
            .runWalkFeedbackUpdateSingleNode(
              selfNodeId,
              trustNodes
            )
            .edges
            .map(e => idxMap(e.dst) -> e.trust) //.flatMap(e => idxMap.get(e.dst).map(i => i -> e.trust) )//todo bug here
            .toMap

          _ <- logger.info(s"TrustManager.idMappedScores: ${idMappedScores.toString()}")
          _ <- storedReputation.modify(_ => (idMappedScores, ()))
          _ <- predictedReputation.modify(_ => (idMappedScores, ()))
        } yield ()
      } else logger.debug("Skipping trust score update")
    }

  def getPredictedReputation: F[Map[Id, Double]] = predictedReputation.get

  def getStoredReputation: F[Map[Id, Double]] = {

    def filterFn(peers: Map[Id, PeerData])(id: Id): Boolean =
      !peers.get(id).exists(p => NodeState.offlineStates.contains(p.peerMetadata.nodeState))

    clusterStorage.getPeers.flatMap { peers =>
      storedReputation.get.map(
        reputation => peers.mapValues(_ => 1d) + (nodeId -> 1d) ++ reputation.filterKeys(filterFn(peers))
      )
    }
  }

  def updateStoredReputation(o: Observation): F[Unit] = {
    val score = TrustManager.observationScoring(o.signedObservationData.data.event)
    val id = o.signedObservationData.data.id

    storedReputation.modify { reputation =>
      //    val updated = Math.max(reputation.getOrElse(id, 1d) + score, -1d)
      val updated = 1d
      (reputation + (id -> updated), ())
    }
  }

//  private def calculateReputation(snapshots: List[StoredSnapshot]): Map[Id, Double] =
//    snapshots
//      .flatMap(_.checkpointCache.flatMap(_.checkpointBlock.flatMap(_.observations.toList)))
//      .groupBy(_.signedObservationData.data.id)
//      .mapValues(_.size.toDouble) // TODO: wkoszycki add conversion List[Observation] -> Score
}

object TrustManager {

  def observationScoring(event: ObservationEvent): Double =
    event match {
      case _: CheckpointBlockWithMissingParents => -0.1
      case _: CheckpointBlockWithMissingSoe     => -0.1
      case _: RequestTimeoutOnConsensus         => -0.1
      case _: RequestTimeoutOnResolving         => -0.1
      case _: CheckpointBlockInvalid            => -0.1
      case _                                    => 0d
    }

  def calculateScoringMap(scores: List[Id]): Map[Id, Int] =
    scores.sortBy { _.hex }.zipWithIndex.toMap
  //todo need UUID Int for ID, since when nodes join/leave they will "take" on old score.
  // Instead, make fixed-size cache of all Full nodes, update when nodes join/leave. Clear the old data too

  def calculateIdxMaps(scores: List[TrustDataInternal]): (Map[Id, Int], Map[Int, Id]) = {
    val allNodeIds = scores.flatMap(_.view.keySet).distinct
    val scoringMap = TrustManager.calculateScoringMap(allNodeIds)
    val idxMap = scoringMap.map(_.swap)
    (scoringMap, idxMap)
  }

  def calculateTrustNodes(
    scores: List[TrustDataInternal],
    currentNodeId: Id,
    scoringMap: Map[Id, Int]
  ): List[TrustNode] =
    scores.map {
      case TrustDataInternal(id, peerScores) =>
        val selfIdx = scoringMap(id)
        TrustNode(
          selfIdx,
          0d,
          0d,
          peerScores.map {
            case (peerId, score) => TrustEdge(selfIdx, scoringMap(peerId), score, id == currentNodeId)
          }.toSeq.distinct
        )
    }

  def apply[F[_]: Concurrent](nodeId: Id, clusterStorage: ClusterStorageAlgebra[F]): TrustManager[F] =
    new TrustManager[F](nodeId, clusterStorage)
}
