package org.constellation.domain.checkpointBlock

import org.apache.commons.collections.set.ListOrderedSet
import org.constellation.schema.Height
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}

import scala.collection.SortedSet
import scala.collection.immutable.Queue

trait CheckpointStorageAlgebra[F[_]] {

  def countCheckpoints: F[Int]
  def countAccepted: F[Int]
  def countAwaiting: F[Int]
  def countWaitingForAcceptance: F[Int]
  def countWaitingForResolving: F[Int]
  def countWaitingForAcceptanceAfterDownload: F[Int]
  def countInAcceptance: F[Int]
  def countInSnapshot: F[Int]

  def getAcceptanceQueue: F[Set[String]]
  def setAcceptanceQueue(q: Set[String]): F[Unit]
  def pullForAcceptance(): F[Option[String]]

  def persistCheckpoint(checkpoint: CheckpointCache): F[Unit]
  def getCheckpoint(soeHash: String): F[Option[CheckpointCache]]
  def setCheckpoints(checkpoints: Map[String, CheckpointCache]): F[Unit]
  def getCheckpoints: F[Map[String, CheckpointCache]]
  def updateCheckpointHeight(soeHash: String, height: Option[Height]): F[Unit]

  def removeCheckpoint(soeHash: String): F[Unit]
  def removeCheckpoints(soeHashes: Set[String]): F[Unit]

  def isCheckpointInAcceptance(soeHash: String): F[Boolean]
  def isCheckpointAccepted(soeHash: String): F[Boolean]
  def isCheckpointWaitingForAcceptance(soeHash: String): F[Boolean]
  def isCheckpointAwaiting(soeHash: String): F[Boolean]

  def acceptCheckpoint(soeHash: String): F[Unit]
  def existsCheckpoint(soeHash: String): F[Boolean]
  def markWaitingForAcceptance(soeHash: String): F[Unit]
  def markForAcceptance(soeHash: String): F[Unit]
  def unmarkFromAcceptance(soeHash: String): F[Unit]
  def getWaitingForAcceptance: F[Set[CheckpointCache]]
  def setWaitingForAcceptance(soeHashes: Set[String]): F[Unit]

  def markForAcceptanceAfterDownload(cb: CheckpointCache): F[Unit]
  def getCheckpointsForAcceptanceAfterDownload: F[Set[CheckpointCache]]
  def unmarkForAcceptanceAfterDownload(soeHash: String): F[Unit]

  def getAccepted: F[Set[String]]
  def setAccepted(soeHashes: Set[String]): F[Unit]

  def isInSnapshot(soeHash: String): F[Boolean]
  def markInSnapshot(soeHash: String, height: Long): F[Unit]
  def markInSnapshot(soeHashes: Set[(String, Long)]): F[Unit]
  def setInSnapshot(soeHashes: Set[(String, Long)]): F[Unit]
  def getInSnapshot: F[Set[(String, Long)]]

  def getParentSoeHashes(soeHash: String): F[Option[List[String]]]
  def getParents(soeHash: String): F[Option[List[CheckpointCache]]]
  def areParentsAccepted(checkpoint: CheckpointCache): F[Boolean]
  def calculateHeight(soeHash: String): F[Option[Height]]
  def calculateHeight(checkpointBlock: CheckpointBlock): F[Option[Height]]

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
