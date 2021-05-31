package org.constellation.domain.healthcheck

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Sync
import cats.syntax.all._
import org.constellation.domain.healthcheck.HealthCheckConsensus._
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase._
import org.constellation.domain.healthcheck.HealthCheckKey.{MissingProposalHealthCheckKey, PingHealthCheckKey}
import org.constellation.domain.healthcheck.HealthCheckStatus.{
  GotPeerProposalAtHeight,
  MissingOwnProposalAtHeight,
  MissingPeerProposalAtHeight,
  MissingProposalHealthCheckStatus,
  PeerAvailable,
  PeerPingHealthCheckStatus,
  PeerUnresponsive,
  UnknownPeer
}
import org.constellation.domain.healthcheck.ping.ReconciliationRound._
import org.constellation.p2p.PeerData
import org.constellation.schema.{Id, NodeState}

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

  def logHealthCheckKey(key: HealthCheckKey): String = key match {
    case PingHealthCheckKey(id)                    => id.short
    case MissingProposalHealthCheckKey(id, height) => s"(${id.short}, $height)"
  }

  def logHealthCheckKeys[K <: HealthCheckKey](keys: Set[K]): String =
    keys.map(logHealthCheckKey).toString

  def logRoundIdWithOwner(roundId: HealthcheckRoundId): String =
    s"roundId=${logRoundId(roundId)} owner=${logRoundOwner(roundId)}"
  def logRoundIdShort(roundId: HealthcheckRoundId): String = roundId.roundId.id.take(8)

  def logKeyToRoundIdsMapping[K <: HealthCheckKey](idToRoundsIds: Map[K, Set[HealthcheckRoundId]]): String =
    idToRoundsIds.map { case (key, roundIds) => logHealthCheckKey(key) -> logRoundIds(roundIds) }.toString()

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

  def logPeerPingHealthCheckStatus(status: PeerPingHealthCheckStatus): String =
    status match {
      case PeerAvailable(id)    => s"PeerAvailable(${logId(id)})"
      case PeerUnresponsive(id) => s"PeerUnresponsive(${logId(id)})"
      case UnknownPeer(id)      => s"UnknownPeer(${logId(id)})"
    }

  def logMissingProposalHealthCheckStatus(status: MissingProposalHealthCheckStatus): String =
    status match {
      case GotPeerProposalAtHeight(id, height)     => s"GotPeerProposalAtHeight(${logId(id)}, $height)"
      case MissingPeerProposalAtHeight(id, height) => s"MissingPeerProposalAtHeight(${logId(id)}, $height)"
      case MissingOwnProposalAtHeight(id, height)  => s"MissingOwnProposalAtHeight(${logId(id)}, $height)"
    }

  def logPeerHealthCheckStatus(status: HealthCheckStatus): String =
    status match {
      case status: PeerPingHealthCheckStatus        => logPeerPingHealthCheckStatus(status)
      case status: MissingProposalHealthCheckStatus => logMissingProposalHealthCheckStatus(status)
    }

  def logConsensusHealthStatus[K <: HealthCheckKey, A <: HealthCheckStatus](
    healthStatus: ConsensusHealthStatus[K, A]
  ): String =
    healthStatus match {
      case PingHealthStatus(key, _, checkingPeerId, roundId, status, clusterState) =>
        s"key=${logHealthCheckKey(key)} checkingId=${logId(checkingPeerId)} ${logRoundIdWithOwner(roundId)} status=${logPeerHealthCheckStatus(status)} clusterState=${logClusterState(clusterState)}"
      case MissingProposalHealthStatus(key, _, checkingPeerId, roundId, status, clusterState) =>
        s"key=${logHealthCheckKey(key)} checkingId=${logId(checkingPeerId)} ${logRoundIdWithOwner(roundId)} status=${logPeerHealthCheckStatus(status)} clusterState=${logClusterState(clusterState)}"
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

  def logMissingRoundDataForId[K <: HealthCheckKey, A <: HealthCheckStatus](
    idToRoundData: Map[Id, RoundData[K, A]]
  ): String =
    idToRoundData.map {
      case (id, RoundData(_, healthStatus, receivedProposal)) =>
        logId(id) -> s"peerReceivedMyProposal=$receivedProposal, IReceivedPeersProposal=${healthStatus.nonEmpty}"
    }.toString()

  // Logging and metrics
  def logHealthcheckConsensusDecision[F[_]: Sync, K <: HealthCheckKey](decision: HealthcheckConsensusDecision[K])(
    metrics: PrefixedHealthCheckMetrics[F],
    logger: PrefixedHealthCheckLogger[F]
  ): F[Unit] =
    decision match {
      case NegativeOutcome(
          key,
          allPeers,
          remainingPeers,
          removedPeers,
          positiveOutcomePercentage,
          positiveOutcomeSize,
          negativeOutcomePercentage,
          negativeOutcomeSize,
          parallelRounds,
          roundIds
          ) =>
        logger.debug(
          s"HealthcheckConsensus decision for id=${logHealthCheckKey(key)} is: NegativeOutcome remainingPeers(${remainingPeers.size})=${logIds(
            remainingPeers
          )} removedPeers(${removedPeers.size})=${logIdToPeerRemovalReasonsMapping(
            removedPeers
          )} allPeers(${allPeers.size})=${logIds(allPeers)} positiveOutcomePercentage=$positiveOutcomePercentage positiveOutcomeSize=$positiveOutcomeSize negativeOutcomePercentage=$negativeOutcomePercentage negativeOutcomeSize=$negativeOutcomeSize parallelRounds=${logKeyToRoundIdsMapping(parallelRounds)} roundIds=${logRoundIds(roundIds)}"
        ) >> metrics.incrementMetricAsync("decision_NegativeOutcome")
      case PositiveOutcome(
          key,
          allPeers,
          remainingPeers,
          removedPeers,
          positiveOutcomePercentage,
          positiveOutcomeSize,
          negativeOutcomePercentage,
          negativeOutcomeSize,
          parallelRounds,
          roundIds
          ) =>
        logger.debug(
          s"HealthcheckConsensus decision for id=${logHealthCheckKey(key)} is: PositiveOutcome remainingPeers(${remainingPeers.size})=${logIds(
            remainingPeers
          )} removedPeers(${removedPeers.size})=${logIdToPeerRemovalReasonsMapping(
            removedPeers
          )} allPeers(${allPeers.size})=${logIds(allPeers)} positiveOutcomePercentage=$positiveOutcomePercentage positiveOutcomeSize=$positiveOutcomeSize negativeOutcomePercentage=$negativeOutcomePercentage negativeOutcomeSize=$negativeOutcomeSize parallelRounds=${logKeyToRoundIdsMapping(parallelRounds)} roundIds=${logRoundIds(roundIds)}"
        ) >> metrics.incrementMetricAsync("decision_PositiveOutcome")
    }

  def incrementFatalIntegrityHealthcheckError[F[_]: Sync](metrics: PrefixedHealthCheckMetrics[F]): F[Unit] =
    metrics.incrementMetricAsync("fatalIntegrityErrorOccurred_total")

  def incrementSuspiciousHealthcheckWarning[F[_]: Sync](metrics: PrefixedHealthCheckMetrics[F]): F[Unit] =
    metrics.incrementMetricAsync("suspiciousWarningOccurred_total")

  def logSendProposalError[F[_]: Sync](
    error: SendProposalError
  )(metrics: PrefixedHealthCheckMetrics[F], logger: PrefixedHealthCheckLogger[F]): F[Unit] =
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
