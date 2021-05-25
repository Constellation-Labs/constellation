package org.constellation.domain.checkpointBlock

import org.constellation.schema.Height
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, CheckpointCacheMetadata}
import org.constellation.schema.edge.SignedObservationEdge

trait CheckpointStorageAlgebra[F[_]] {

  def persistCheckpoint(checkpoint: CheckpointCache): F[Unit]
  def getCheckpoint(soeHash: String): F[Option[CheckpointCache]]
  def updateCheckpointHeight(soeHash: String, height: Option[Height]): F[Unit]

  def removeCheckpoint(soeHash: String): F[Unit]
  def removeCheckpoints(soeHashes: List[String]): F[Unit]

  def isCheckpointInAcceptance(soeHash: String): F[Boolean]
  def isCheckpointAccepted(soeHash: String): F[Boolean]
  def isCheckpointWaitingForAcceptance(soeHash: String): F[Boolean]
  def isCheckpointAwaiting(soeHash: String): F[Boolean]

  def acceptCheckpoint(soeHash: String, height: Option[Height]): F[Unit]
  def existsCheckpoint(soeHash: String): F[Boolean]
  def markForAcceptance(soeHash: String): F[Unit]
  def unmarkFromAcceptance(soeHash: String): F[Unit]

  def getParentSoeHashes(soeHash: String): F[Option[List[String]]]
  def getParents(soeHash: String): F[Option[List[CheckpointCache]]]
  def areParentsAccepted(checkpoint: CheckpointCache): F[Boolean]
  def calculateHeight(soeHash: String): F[Option[Height]]

  def markAsAwaiting(soeHash: String): F[Unit]
  def getAwaiting: F[Set[String]]

  // *** //

  def markCheckpointForResolving(soeHash: String): F[Unit]

  def unmarkCheckpointForResolving(soeHash: String): F[Unit]

}
