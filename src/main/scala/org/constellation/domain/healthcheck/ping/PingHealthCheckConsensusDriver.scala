package org.constellation.domain.healthcheck.ping

import cats.data.{NonEmptyList, NonEmptySet}
import org.constellation.domain.healthcheck.HealthCheckConsensus._
import org.constellation.domain.healthcheck.HealthCheckKey.PingHealthCheckKey
import org.constellation.domain.healthcheck.HealthCheckConsensusTypeDriver
import org.constellation.domain.healthcheck.HealthCheckStatus.{PeerAvailable, PeerPingHealthCheckStatus}
import org.constellation.schema.{Id, NodeState}

import scala.util.Try

object PingHealthCheckConsensusDriver
    extends HealthCheckConsensusTypeDriver[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus] {

  override val removePeersWithParallelRound: Boolean = true

  def getOwnConsensusHealthStatus(
    key: PingHealthCheckKey,
    status: PeerPingHealthCheckStatus,
    checkedPeer: CheckedPeer,
    checkingPeerId: Id,
    roundId: HealthcheckRoundId,
    clusterState: Map[Id, NodeState]
  ): PingHealthStatus = PingHealthStatus(
    key = key,
    checkedPeer = checkedPeer,
    checkingPeerId = checkingPeerId,
    roundId = roundId,
    status = status,
    clusterState = clusterState
  )

  override def isPositiveOutcome(status: PeerPingHealthCheckStatus): Boolean =
    status match {
      case PeerAvailable(_) => true
      case _                => false
    }

  override def calculateConsensusOutcome(
    ownId: Id,
    key: PingHealthCheckKey,
    ownHealthStatus: PeerPingHealthCheckStatus,
    peersData: Map[Id, RoundData[PingHealthCheckKey, PeerPingHealthCheckStatus]],
    roundIds: NonEmptySet[HealthcheckRoundId],
    parallelRounds: Map[PingHealthCheckKey, Set[HealthcheckRoundId]],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]]
  ): HealthcheckConsensusDecision[PingHealthCheckKey] = {
    val peersRemaining = peersData.keySet ++ Set(ownId)
    val allPeers = peersRemaining ++ parallelRounds.keySet.map(_.id) ++ removedPeers.keySet
    val allReceivedStatuses = peersData.values.toList.flatMap(_.healthStatus.map(_.status)) ++ List(ownHealthStatus)
    val (positive, negative) = allReceivedStatuses.partition(isPositiveOutcome)
    val positiveSize = BigDecimal(positive.size)
    val negativeSize = BigDecimal(negative.size)
    val remainingPeersSize = BigDecimal(peersRemaining.size)
    // should we divide by all known peers, or only the ones that remain in the consensus round?
    val positiveOutcomePercentage = Try(positiveSize / remainingPeersSize).toOption
      .getOrElse(if (isPositiveOutcome(ownHealthStatus)) BigDecimal(1) else BigDecimal(0))
    val negativeOutcomePercentage = Try(negativeSize / remainingPeersSize).toOption
      .getOrElse(if (isPositiveOutcome(ownHealthStatus)) BigDecimal(0) else BigDecimal(1))

    if (positiveOutcomePercentage >= BigDecimal(0.5d))
      PositiveOutcome(
        key = key,
        allPeers = allPeers,
        remainingPeers = peersRemaining,
        removedPeers = removedPeers,
        positiveOutcomePercentage = positiveOutcomePercentage,
        positiveOutcomeSize = positiveSize,
        negativeOutcomePercentage = negativeOutcomePercentage,
        negativeOutcomeSize = negativeSize,
        parallelRounds = parallelRounds,
        roundIds = roundIds
      )
    else
      NegativeOutcome(
        key = key,
        allPeers = allPeers,
        remainingPeers = peersRemaining,
        removedPeers = removedPeers,
        positiveOutcomePercentage = positiveOutcomePercentage,
        positiveOutcomeSize = positiveSize,
        negativeOutcomePercentage = negativeOutcomePercentage,
        negativeOutcomeSize = negativeSize,
        parallelRounds = parallelRounds,
        roundIds = roundIds
      )
  }
}
