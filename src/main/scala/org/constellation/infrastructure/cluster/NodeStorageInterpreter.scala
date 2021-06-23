package org.constellation.infrastructure.cluster

import cats.syntax.all._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.p2p.SetStateResult
import org.constellation.schema.NodeState

class NodeStorageInterpreter[F[_]](nodeInitialState: NodeState)(implicit F: Sync[F]) extends NodeStorageAlgebra[F] {
  private val ownJoinedHeight: Ref[F, Option[Long]] = Ref.unsafe(None)
  private val participatedInGenesisFlow: Ref[F, Option[Boolean]] = Ref.unsafe(None)
  private val participatedInRollbackFlow: Ref[F, Option[Boolean]] = Ref.unsafe(None)
  private val joinedAsInitialFacilitator: Ref[F, Option[Boolean]] = Ref.unsafe(None)

  private val nodeState: Ref[F, NodeState] = Ref.unsafe(nodeInitialState)

  def getOwnJoinedHeight: F[Option[Long]] =
    ownJoinedHeight.get

  def setOwnJoinedHeight(height: Long): F[Unit] =
    ownJoinedHeight.modify { h =>
      (h.getOrElse(height).some, ())
    }

  def clearOwnJoinedHeight(): F[Unit] =
    ownJoinedHeight.modify { _ =>
      (None, ())
    }

  def didParticipateInGenesisFlow: F[Option[Boolean]] =
    participatedInGenesisFlow.get

  def setParticipatedInGenesisFlow(participated: Boolean): F[Unit] =
    participatedInGenesisFlow.modify { p =>
      (p.getOrElse(participated).some, ())
    }

  def didParticipateInRollbackFlow: F[Option[Boolean]] =
    participatedInRollbackFlow.get

  def setParticipatedInRollbackFlow(participated: Boolean): F[Unit] =
    participatedInRollbackFlow.modify { p =>
      (p.getOrElse(participated).some, ())
    }

  def didJoinAsInitialFacilitator: F[Option[Boolean]] =
    joinedAsInitialFacilitator.get

  def setJoinedAsInitialFacilitator(joined: Boolean): F[Unit] =
    joinedAsInitialFacilitator.modify { j =>
      (j.getOrElse(joined).some, ())
    }

  def getNodeState: F[NodeState] =
    nodeState.get

  def setNodeState(state: NodeState): F[Unit] =
    nodeState.modify { _ =>
      (state, ())
    }

  def compareAndSet(expected: Set[NodeState], newState: NodeState): F[SetStateResult] =
    nodeState.modify { current =>
      if (expected.contains(current)) (newState, SetStateResult(current, isNewSet = true))
      else (current, SetStateResult(current, isNewSet = false))
    }
}
