package org.constellation.domain.healthcheck

import cats.data.{NonEmptyList, NonEmptySet}
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  CheckedPeer,
  ConsensusHealthStatus,
  HealthcheckConsensusDecision,
  HealthcheckRoundId,
  PeerRemovalReason,
  RoundData
}
import org.constellation.schema.{Id, NodeState}

trait HealthCheckConsensusTypeDriver[K <: HealthCheckKey, A <: HealthCheckStatus, B <: ConsensusHealthStatus[K, A]] {

  val removePeersWithParallelRound: Boolean

  def getOwnConsensusHealthStatus(
    key: K,
    status: A,
    checkedPeer: CheckedPeer,
    checkingPeerId: Id,
    roundId: HealthcheckRoundId,
    clusterState: Map[Id, NodeState]
  ): B

  def isPositiveOutcome(status: A): Boolean

  def calculateConsensusOutcome(
    ownId: Id,
    key: K,
    ownHealthStatus: A,
    peersData: Map[Id, RoundData[K, A]],
    roundIds: NonEmptySet[HealthcheckRoundId],
    parallelRounds: Map[K, Set[HealthcheckRoundId]],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]]
  ): HealthcheckConsensusDecision[K]
}
