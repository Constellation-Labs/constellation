package org.constellation.infrastructure.checkpointBlock

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.mapref.MapRef
import org.constellation.concurrency.MapRefUtils
import org.constellation.concurrency.MapRefUtils.MapRefOps
import org.constellation.concurrency.SetRefUtils.RefOps
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.schema.Height
import org.constellation.schema.checkpoint.CheckpointCache

class CheckpointStorageInterpreter[F[_]]()(implicit F: Concurrent[F]) extends CheckpointStorageAlgebra[F] {

  val checkpoints: MapRef[F, String, Option[CheckpointCache]] =
    MapRefUtils.ofConcurrentHashMap() // Consider cache and time-removal

  val waitingForAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val inAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val accepted: Ref[F, Set[String]] = Ref.unsafe(Set.empty)

  val awaiting: Ref[F, Set[String]] = Ref.unsafe(Set())

  val waitingForResolving: Ref[F, Set[String]] = Ref.unsafe(Set())

  val waitingForAcceptanceAfterDownload: Ref[F, Set[String]] = Ref.unsafe(Set())

  val tips: Ref[F, Set[String]] = Ref.unsafe(Set())
  val usages: MapRef[F, String, Option[Set[String]]] = MapRefUtils.ofConcurrentHashMap()

  def persistCheckpoint(checkpoint: CheckpointCache): F[Unit] =
    checkpoints(checkpoint.checkpointBlock.soeHash).set(checkpoint.some)

  def removeCheckpoints(soeHashes: Set[String]): F[Unit] =
    soeHashes.toList.traverse(removeCheckpoint).void

  def removeCheckpoint(soeHash: String): F[Unit] =
    checkpoints(soeHash).set(none) >>
      accepted.remove(soeHash) >>
      waitingForAcceptance.remove(soeHash) >>
      tips.remove(soeHash) >>
      usages(soeHash).set(none)

  def isCheckpointInAcceptance(soeHash: String): F[Boolean] =
    inAcceptance.exists(soeHash)

  def isCheckpointWaitingForAcceptance(soeHash: String): F[Boolean] =
    waitingForAcceptance.exists(soeHash)

  def isCheckpointAwaiting(soeHash: String): F[Boolean] =
    awaiting.exists(soeHash)

  def acceptCheckpoint(soeHash: String, height: Option[Height]): F[Unit] =
    accepted.add(soeHash) >>
      inAcceptance.remove(soeHash) >>
      updateCheckpointHeight(soeHash, height)

  def updateCheckpointHeight(soeHash: String, height: Option[Height]): F[Unit] =
    checkpoints(soeHash).update(_.map { cb =>
      cb.copy(height = height)
    })

  def existsCheckpoint(soeHash: String): F[Boolean] =
    checkpoints(soeHash).get.map(_.nonEmpty)

  def markWaitingForAcceptance(soeHash: String): F[Unit] =
    waitingForAcceptance.add(soeHash)

  def markForAcceptance(soeHash: String): F[Unit] =
    inAcceptance.add(soeHash) >>
      waitingForAcceptance.remove(soeHash)

  def unmarkFromAcceptance(soeHash: String): F[Unit] =
    inAcceptance.remove(soeHash)

  def markForAcceptanceAfterDownload(soeHash: String): F[Unit] =
    waitingForAcceptanceAfterDownload.add(soeHash)

  def getCheckpointsForAcceptanceAfterDownload: F[List[CheckpointCache]] =
    waitingForAcceptanceAfterDownload.get.flatMap(_.toList.traverse { getCheckpoint }).map(_.flatten)

  def getAcceptedCheckpoints: F[Set[String]] =
    accepted.get

  def areParentsAccepted(checkpoint: CheckpointCache): F[Boolean] =
    checkpoint.checkpointBlock.parentSOEHashes.distinct.toList.traverse { isCheckpointAccepted }
      .map(_.forall(_ == true))

  def isCheckpointAccepted(soeHash: String): F[Boolean] =
    accepted.exists(soeHash)

  def markAsAwaiting(soeHash: String): F[Unit] =
    awaiting.add(soeHash)

  def getAwaiting: F[Set[String]] = awaiting.get

  def setAwaiting(a: Set[String]): F[Unit] =
    awaiting.set(a)

  def calculateHeight(soeHash: String): F[Option[Height]] =
    getParents(soeHash).map {
      _.flatMap { parents =>
        val maybeHeight = parents.flatMap(_.height)
        if (maybeHeight.isEmpty)
          none[Height]
        else
          Height(maybeHeight.map(_.min).min + 1, maybeHeight.map(_.max).max + 1).some
      }
    }

  def getParents(soeHash: String): F[Option[List[CheckpointCache]]] =
    getParentSoeHashes(soeHash)
      .flatMap(_.traverse(_.traverse(getCheckpoint)))
      .map(_.sequence.map(_.flatten).sequence)

  def getParentSoeHashes(soeHash: String): F[Option[List[String]]] =
    getCheckpoint(soeHash).nested.map(_.checkpointBlock.parentSOEHashes.toList).value

  def markCheckpointForResolving(soeHash: String): F[Unit] =
    waitingForResolving.add(soeHash)

  def unmarkCheckpointForResolving(soeHash: String): F[Unit] =
    waitingForResolving.remove(soeHash)

  def isWaitingForResolving(soeHash: String): F[Boolean] =
    waitingForResolving.exists(soeHash)

  def countUsages(soeHash: String): F[Int] =
    usages(soeHash).get.map(_.map(_.size).getOrElse(0))

  def registerUsage(soeHash: String): F[Unit] =
    getParentSoeHashes(soeHash)
      .map(_.sequence.flatten)
      .flatMap {
        _.traverse { parent =>
          usages(parent).update(_.orElse(Set.empty[String].some).map(_ + soeHash))
        }
      }
      .void

  def getUsages: F[Map[String, Set[String]]] =
    usages.toMap

  def setUsages(u: Map[String, Set[String]]): F[Unit] =
    usages.clear >>
      u.keySet.toList.traverse { soeHash =>
        usages(soeHash).set(u(soeHash).some)
      }.void

  def removeUsages(soeHashes: Set[String]): F[Unit] =
    soeHashes.toList.traverse(removeUsage).void

  def removeUsage(soeHash: String): F[Unit] =
    usages(soeHash).set(none)

  def addTip(soeHash: String): F[Unit] =
    tips.add(soeHash)

  def removeTips(soeHashes: Set[String]): F[Unit] =
    soeHashes.toList.traverse(removeTip).void

  def removeTip(soeHash: String): F[Unit] =
    tips.remove(soeHash)

  def setTips(newTips: Set[String]): F[Unit] =
    tips.modify(_ => (newTips, ()))

  def countTips: F[Int] =
    tips.get.map(_.size)

  def getMinTipHeight: F[Long] =
    for {
      tips <- getTips
      minHeights = tips.map(_._2.min)
      minHeight = if (tips.nonEmpty) minHeights.min else 0L
    } yield minHeight

  def getTips: F[Set[(String, Height)]] =
    tips.get
      .flatMap(_.toList.traverse(getCheckpoint))
      .map(
        a =>
          a.flatten.map { checkpoint =>
            (checkpoint.checkpointBlock.soeHash, checkpoint.height.getOrElse(Height(0L, 0L)))
          }
      )
      .map(_.toSet)

  def getCheckpoint(soeHash: String): F[Option[CheckpointCache]] =
    checkpoints(soeHash).get
}
