package org.constellation.domain.healthcheck

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Sync
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  CheckedPeer,
  ConsensusHealthStatus,
  HealthcheckConsensusDecision,
  HealthcheckRoundId,
  PeerOffline,
  PeerOnline,
  PeerRemovalReason,
  RoundData
}
import org.constellation.domain.healthcheck.HealthCheckConsensusManager.{
  DifferentProposalAlreadyProcessedForCheckedId,
  DifferentProposalForRoundIdAlreadyProcessed,
  IdNotPartOfTheConsensus,
  InternalErrorStartingRound,
  ProposalNotProcessedForHistoricalRound,
  SendProposalError
}
import org.constellation.domain.healthcheck.ReconciliationRound.{
  ClusterAlignmentResult,
  NodeAligned,
  NodeInconsistentlySeenAsOnlineOrOffline,
  NodeNotPresentOnAllNodes,
  ReconciliationResult
}
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.p2p.PeerHealthCheck.PeerHealthCheckStatus
import org.constellation.p2p.PeerData
import org.constellation.schema.{Id, NodeState}
import org.constellation.util.Metrics

object HealthCheckLoggingHelper {
  // Simple value to string conversions
  def logRoundIds(roundIds: Set[HealthcheckRoundId]): String = roundIds.map(logRoundIdShort).toString()
  def logRoundIds(roundIds: NonEmptySet[HealthcheckRoundId]): String = logRoundIds(roundIds.toSortedSet)
  def logIdShort(id: Id): String = id.short
  def logId(id: Id): String = id.medium
  def logId(checkedPeer: CheckedPeer): String = checkedPeer.id.medium
  def logId(peerData: PeerData): String = logId(peerData.peerMetadata.id)
  def logIds(ids: Set[Id]): String = ids.map(_.short).toString()
  def logIds(ids: NonEmptySet[Id]): String = logIds(ids.toSortedSet)
  def logRoundOwner(roundId: HealthcheckRoundId): String = logId(roundId.owner)
  def logRoundId(roundId: HealthcheckRoundId): String = roundId.roundId.id

  def logRoundIdWithOwner(roundId: HealthcheckRoundId): String =
    s"roundId=${logRoundId(roundId)} owner=${logRoundOwner(roundId)}"
  def logRoundIdShort(roundId: HealthcheckRoundId): String = roundId.roundId.id.take(8)

  def logIdToRoundIdsMapping(idToRoundsIds: Map[Id, Set[HealthcheckRoundId]]): String =
    idToRoundsIds.map { case (id, roundIds) => logId(id) -> logRoundIds(roundIds) }.toString()

  def logClusterState(clusterState: Map[Id, NodeState]): String =
    clusterState.map { case (id, state) => logIdShort(id) -> state }.toString()

  def logClusterAlignmentResult(clusterAlignmentResult: ClusterAlignmentResult): String =
    clusterAlignmentResult match {
      case NodeAligned =>
        "NodeAligned"
      case NodeNotPresentOnAllNodes(presentOn, notPresentOn) =>
        s"NodeNotPresentOnAllNodes: presentOn=${logIds(presentOn)} notPresentOn=${logIds(notPresentOn)}"
      case NodeInconsistentlySeenAsOnlineOrOffline(onlineOn, offlineOn) =>
        s"NodeInconsistentlySeenAsOnlineOrOffline: onlineOn=${logIds(onlineOn)} offlineOn=${logIds(offlineOn)}"
    }

  def logPeerHealthCheckStatus(status: PeerHealthCheckStatus): String =
    status match {
      case PeerHealthCheck.PeerAvailable(id)    => s"PeerAvailable(${logId(id)})"
      case PeerHealthCheck.PeerUnresponsive(id) => s"PeerUnresponsive(${logId(id)})"
      case PeerHealthCheck.UnknownPeer(id)      => s"UnknownPeer(${logId(id)})"
    }

  def logConsensusHealthStatus(healthStatus: ConsensusHealthStatus): String = {
    val ConsensusHealthStatus(checkedId, checkingPeerId, roundId, status, clusterState) = healthStatus

    s"checkedId=${logId(checkedId)} checkingId=${logId(checkingPeerId)} ${logRoundIdWithOwner(roundId)} status=${logPeerHealthCheckStatus(status)} clusterState=${logClusterState(clusterState)}"
  }

  def logClusterAlignmentResults(clusterAlignmentResults: List[ClusterAlignmentResult]): String =
    clusterAlignmentResults.map(logClusterAlignmentResult).toString()

  def logIdToClusterAlignmentResultsMapping(
    idToClusterAlignmentResultsMapping: Map[Id, List[ClusterAlignmentResult]]
  ): String =
    idToClusterAlignmentResultsMapping.map {
      case (id, alignmentResults) => logIdShort(id) -> logClusterAlignmentResults(alignmentResults)
    }.toString()

  def logIdToClusterAlignmentResultsMapping(
    maybeIdToClusterAlignmentResultsMapping: Option[Map[Id, List[ClusterAlignmentResult]]]
  ): String =
    maybeIdToClusterAlignmentResultsMapping.map(logIdToClusterAlignmentResultsMapping).toString

  def logIdToPeerRemovalReasonsMapping(
    idToPeerRemovalReasonsMapping: Map[Id, NonEmptyList[PeerRemovalReason]]
  ): String =
    idToPeerRemovalReasonsMapping.map {
      case (id, peerRemovalReasons) => logIdShort(id) -> peerRemovalReasons.toString
    }.toString()

  def logReconciliationResult(reconciliationResult: ReconciliationResult): String = {
    val ReconciliationResult(peersToRunHealthCheckFor, misalignedPeers, clusterAlignment) = reconciliationResult

    s"peersToRunHealthcheckFor: ${logIds(peersToRunHealthCheckFor)} misalignedPeers: ${logIds(misalignedPeers)}, clusterAlignment: ${logIdToClusterAlignmentResultsMapping(clusterAlignment)}"
  }

  def logMissingRoundDataForId(idToRoundData: Map[Id, RoundData]): String =
    idToRoundData.map {
      case (id, RoundData(peerData, healthStatus, receivedProposal)) =>
        logId(id) -> s"peerReceivedMyProposal=$receivedProposal, IReceivedPeersProposal=${healthStatus.nonEmpty}"
    }.toString()

  // Logging and metrics
  def logHealthcheckConsensusDecicion[F[_]: Sync](
    decision: HealthcheckConsensusDecision)(
    metrics: Metrics,
    logger: SelfAwareStructuredLogger[F]
  ): F[Unit] =
    decision match {
      case PeerOffline(
          id,
          allPeers,
          remainingPeers,
          removedPeers,
          percentage,
          parallelRounds,
          roundIds,
          isKeepUpRound
          ) =>
        logger.debug(
          s"HealthcheckConsensus decision for id=${logId(id)} is: PeerOffline remainingPeers(${remainingPeers.size})=${logIds(
            remainingPeers
          )} removedPeers(${removedPeers.size})=${logIdToPeerRemovalReasonsMapping(removedPeers)} allPeers(${allPeers.size})=${logIds(
            allPeers
          )} percentage=$percentage parallelRounds=${logIdToRoundIdsMapping(parallelRounds)} roundIds=${logRoundIds(roundIds)} isKeepUpRound=$isKeepUpRound"
        ) >> metrics.incrementMetricAsync("healthcheck_decision_PeerOffline")
      case PeerOnline(
          id,
          allPeers,
          remainingPeers,
          removedPeers,
          percentage,
          parallelRounds,
          roundIds,
          isKeepUpRound
          ) =>
        logger.debug(
          s"HealthcheckConsensus decision for id=${logId(id)} is: PeerOnline remainingPeers(${remainingPeers.size})=${logIds(
            remainingPeers
          )} removedPeers(${removedPeers.size})=${logIdToPeerRemovalReasonsMapping(removedPeers)} allPeers(${allPeers.size})=${logIds(
            allPeers
          )} percentage=$percentage parallelRounds=${logIdToRoundIdsMapping(parallelRounds)} roundIds=${logRoundIds(roundIds)} isKeepUpRound=$isKeepUpRound"
        ) >> metrics.incrementMetricAsync("healthcheck_decision_PeerOnline")
    }

  def incrementFatalIntegrityHealthcheckError[F[_]: Sync](metrics: Metrics): F[Unit] =
    metrics.incrementMetricAsync[F]("healthcheck_fatalIntegrityErrorOccurred_total")

  def incrementSuspiciousHealthcheckWarning[F[_]: Sync](metrics: Metrics): F[Unit] =
    metrics.incrementMetricAsync[F]("healthcheck_suspiciousWarningOccurred_total")

  def logSendProposalError[F[_]: Sync](
    error: SendProposalError
  )(metrics: Metrics, logger: SelfAwareStructuredLogger[F]): F[Unit] =
    error match {
      case DifferentProposalForRoundIdAlreadyProcessed(checkingPeerId, roundIds, roundType) =>
        logger.error(
          s"Different proposal from ${logId(checkingPeerId)} already processed for $roundType with roundIds=${logRoundIds(roundIds)}"
        ) >>
          incrementFatalIntegrityHealthcheckError(metrics)
      case DifferentProposalAlreadyProcessedForCheckedId(checkedId, checkingPeerId, roundIds, roundType) =>
        logger.warn(
          s"Different proposal from ${logId(checkingPeerId)} got already processed for checked id=${logId(checkedId)} for $roundType with roundIds=${logRoundIds(roundIds)}"
        )

      case IdNotPartOfTheConsensus(id, roundIds, roundType) =>
        logger.warn(
          s"Given id=${logId(id)} is not part of the $roundType with roundIds=${logRoundIds(roundIds)}"
        )
      case ProposalNotProcessedForHistoricalRound(roundIds) =>
        logger.error(
          s"Historical round ended without processing peers proposal for the historical round with roundIds=${logRoundIds(roundIds)}"
        ) >>
          incrementFatalIntegrityHealthcheckError(metrics)
      case InternalErrorStartingRound(roundId) =>
        logger.error(
          s"Internal error occured during starting a round with id=${logRoundId(roundId)}"
        ) >>
          incrementFatalIntegrityHealthcheckError(metrics)
      case error =>
        logger.debug(error)(s"Error processing a proposal.")
    }
}
