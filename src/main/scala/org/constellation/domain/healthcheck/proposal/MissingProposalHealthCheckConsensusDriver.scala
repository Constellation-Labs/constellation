package org.constellation.domain.healthcheck.proposal

import cats.data.{NonEmptyList, NonEmptySet}
import org.constellation.domain.healthcheck.HealthCheckConsensus._
import org.constellation.domain.healthcheck.HealthCheckStatus.{
  GotPeerProposalAtHeight,
  MissingOwnProposalAtHeight,
  MissingProposalHealthCheckStatus
}
import org.constellation.domain.healthcheck.HealthCheckConsensusTypeDriver
import org.constellation.domain.healthcheck.HealthCheckKey.MissingProposalHealthCheckKey
import org.constellation.schema.{Id, NodeState}

import scala.util.Try

object MissingProposalHealthCheckConsensusDriver
    extends HealthCheckConsensusTypeDriver[
      MissingProposalHealthCheckKey,
      MissingProposalHealthCheckStatus,
      MissingProposalHealthStatus
    ] {

  override val removePeersWithParallelRound: Boolean = false

  override def getOwnConsensusHealthStatus(
    key: MissingProposalHealthCheckKey,
    status: MissingProposalHealthCheckStatus,
    checkedPeer: CheckedPeer,
    checkingPeerId: Id,
    roundId: HealthcheckRoundId,
    clusterState: Map[Id, NodeState]
  ): MissingProposalHealthStatus = MissingProposalHealthStatus(
    key = key,
    checkedPeer = checkedPeer,
    checkingPeerId = checkingPeerId,
    roundId = roundId,
    status = status,
    clusterState = clusterState
  )

  override def isPositiveOutcome(status: MissingProposalHealthCheckStatus): Boolean = status match {
    case GotPeerProposalAtHeight(_, _) | MissingOwnProposalAtHeight(_, _) => true
    case _                                                                => false
  }

  override def calculateConsensusOutcome(
    ownId: Id,
    key: MissingProposalHealthCheckKey,
    ownHealthStatus: MissingProposalHealthCheckStatus,
    peersData: Map[Id, RoundData[MissingProposalHealthCheckKey, MissingProposalHealthCheckStatus]],
    roundIds: NonEmptySet[HealthcheckRoundId],
    parallelRounds: Map[MissingProposalHealthCheckKey, Set[HealthcheckRoundId]],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]]
  ): HealthcheckConsensusDecision[MissingProposalHealthCheckKey] = {
    val peersRemaining = peersData.keySet ++ Set(ownId)
    val allPeers = peersRemaining ++ parallelRounds.keySet.map(_.id) ++ removedPeers.keySet
    val allReceivedStatuses = peersData.values.toList.flatMap(_.healthStatus.map(_.status)) ++ List(ownHealthStatus)
    val proposalExists = allReceivedStatuses.exists(_.isInstanceOf[GotPeerProposalAtHeight])
    val (positive, negative) = allReceivedStatuses.partition(isPositiveOutcome)
    val positiveSize = BigDecimal(positive.size)
    val negativeSize = BigDecimal(negative.size)
    val remainingPeersSize = BigDecimal(peersRemaining.size)
    // should we divide by all known peers, or only the ones that remain in the consensus round?
    val positiveOutcomePercentage = Try(positiveSize / remainingPeersSize).toOption
      .getOrElse(if (isPositiveOutcome(ownHealthStatus)) BigDecimal(1) else BigDecimal(0))
    val negativeOutcomePercentage = Try(negativeSize / remainingPeersSize).toOption
      .getOrElse(if (isPositiveOutcome(ownHealthStatus)) BigDecimal(0) else BigDecimal(1))

    if (positiveOutcomePercentage >= BigDecimal(0.5d) || proposalExists)
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
