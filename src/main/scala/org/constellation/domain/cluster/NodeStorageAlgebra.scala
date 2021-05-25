package org.constellation.domain.cluster

import org.constellation.p2p.SetStateResult
import org.constellation.schema.NodeState

trait NodeStorageAlgebra[F[_]] {
  def getOwnJoinedHeight: F[Option[Long]]
  def setOwnJoinedHeight(height: Long): F[Unit]
  def clearOwnJoinedHeight(): F[Unit]

  def didParticipateInGenesisFlow: F[Option[Boolean]]
  def setParticipatedInGenesisFlow(participated: Boolean): F[Unit]

  def didParticipateInRollbackFlow: F[Option[Boolean]]
  def setParticipatedInRollbackFlow(participated: Boolean): F[Unit]

  def didJoinAsInitialFacilitator: F[Option[Boolean]]
  def setJoinedAsInitialFacilitator(joined: Boolean): F[Unit]

  def getNodeState: F[NodeState]
  def setNodeState(state: NodeState): F[Unit] // TODO: consider compareAndSet
  def compareAndSet(expected: Set[NodeState], newState: NodeState): F[SetStateResult]

}
