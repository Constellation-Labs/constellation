package org.constellation.domain.checkpointBlock

import org.constellation.schema.Height
import org.constellation.schema.checkpoint.CheckpointCache

trait CheckpointStorageAlgebra[F[_]] {

  def persistCheckpoint(checkpoint: CheckpointCache): F[Unit]
  def getCheckpoint(soeHash: String): F[Option[CheckpointCache]]
  def updateCheckpointHeight(soeHash: String, height: Option[Height]): F[Unit]

  def removeCheckpoint(soeHash: String): F[Unit]
  def removeCheckpoints(soeHashes: Set[String]): F[Unit]

  def isCheckpointInAcceptance(soeHash: String): F[Boolean]
  def isCheckpointAccepted(soeHash: String): F[Boolean]
  def isCheckpointWaitingForAcceptance(soeHash: String): F[Boolean]
  def isCheckpointAwaiting(soeHash: String): F[Boolean]

  def acceptCheckpoint(soeHash: String, height: Option[Height]): F[Unit]
  def existsCheckpoint(soeHash: String): F[Boolean]
  def markWaitingForAcceptance(soeHash: String): F[Unit]
  def markForAcceptance(soeHash: String): F[Unit]
  def unmarkFromAcceptance(soeHash: String): F[Unit]

  def markForAcceptanceAfterDownload(soeHash: String): F[Unit]
  def getCheckpointsForAcceptanceAfterDownload: F[List[CheckpointCache]]

  def getAcceptedCheckpoints: F[Set[String]]

  def getParentSoeHashes(soeHash: String): F[Option[List[String]]]
  def getParents(soeHash: String): F[Option[List[CheckpointCache]]]
  def areParentsAccepted(checkpoint: CheckpointCache): F[Boolean]
  def calculateHeight(soeHash: String): F[Option[Height]]

  def markAsAwaiting(soeHash: String): F[Unit]
  def getAwaiting: F[Set[String]]
  def setAwaiting(awaiting: Set[String]): F[Unit]

  def markCheckpointForResolving(soeHash: String): F[Unit]
  def unmarkCheckpointForResolving(soeHash: String): F[Unit]
  def isWaitingForResolving(soeHash: String): F[Boolean]

  def countUsages(soeHash: String): F[Int]
  def registerUsage(soeHash: String): F[Unit]
  def getUsages: F[Map[String, Set[String]]]
  def setUsages(usages: Map[String, Set[String]]): F[Unit]
  def removeUsage(soeHash: String): F[Unit]
  def removeUsages(soeHashes: Set[String]): F[Unit]

  def getTips: F[Set[(String, Height)]]
  def addTip(soeHash: String): F[Unit]
  def removeTip(soeHash: String): F[Unit]
  def removeTips(soeHashes: Set[String]): F[Unit]
  def setTips(tips: Set[String]): F[Unit]
  def countTips: F[Int]

  def getMinTipHeight: F[Long]

}
