package org.constellation.infrastructure.checkpointBlock

import cats.effect.Concurrent
import cats.effect.concurrent.{Ref, Semaphore}
import cats.syntax.all._
import io.chrisdavenport.mapref.MapRef
import org.constellation.concurrency.MapRefUtils
import org.constellation.concurrency.MapRefUtils.MapRefOps
import org.constellation.concurrency.SetRefUtils.RefOps
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.genesis.Genesis
import org.constellation.schema.Height
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}

class CheckpointStorageInterpreter[F[_]](implicit F: Concurrent[F]) extends CheckpointStorageAlgebra[F] {

  val checkpoints: MapRef[F, String, Option[CheckpointCache]] =
    MapRefUtils.ofConcurrentHashMap() // Consider cache and time-removal

  val waitingForAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val inAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val accepted: Ref[F, Set[String]] = Ref.unsafe(Set.empty)

  val queueSemaphore: Semaphore[F] = createSemaphore[F](1)
  val acceptanceQueue: Ref[F, Set[String]] = Ref.unsafe(Set.empty)

  val awaiting: Ref[F, Set[String]] = Ref.unsafe(Set.empty)

  val waitingForResolving: Ref[F, Set[String]] = Ref.unsafe(Set.empty)

  val waitingForAcceptanceAfterDownload: Ref[F, Set[CheckpointCache]] = Ref.unsafe(Set.empty)

  val inSnapshot: Ref[F, Set[(String, Long)]] = Ref.unsafe(Set.empty)

  val tips: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val usages: MapRef[F, String, Option[Set[String]]] = MapRefUtils.ofConcurrentHashMap()

  def countCheckpoints: F[Int] =
    checkpoints.toMap.map(_.size)

  def countAccepted: F[Int] =
    accepted.get.map(_.size)

  def countAwaiting: F[Int] =
    awaiting.get.map(_.size)

  def countWaitingForAcceptance: F[Int] =
    waitingForAcceptance.get.map(_.size)

  def countWaitingForResolving: F[Int] =
    waitingForResolving.get.map(_.size)

  def countWaitingForAcceptanceAfterDownload: F[Int] =
    waitingForAcceptanceAfterDownload.get.map(_.size)

  def countInAcceptance: F[Int] =
    inAcceptance.get.map(_.size)

  def countInSnapshot: F[Int] =
    inSnapshot.get.map(_.size)

  def getAcceptanceQueue: F[Set[String]] =
    queueSemaphore.withPermit {
      acceptanceQueue.get
    }

  def setAcceptanceQueue(q: Set[String]): F[Unit] =
    queueSemaphore.withPermit {
      acceptanceQueue.set(q)
    }

  def pullForAcceptance(): F[Option[String]] =
    queueSemaphore.withPermit {
      acceptanceQueue.modify { q =>
        if (q.nonEmpty) {
          (q.tail, q.headOption)
        } else {
          (Set.empty, none[String])
        }
      }
    }

  def persistCheckpoint(checkpoint: CheckpointCache): F[Unit] =
    checkpoints(checkpoint.checkpointBlock.soeHash).set(checkpoint.some)

  def removeCheckpoints(soeHashes: Set[String]): F[Unit] =
    soeHashes.toList.traverse(removeCheckpoint).void

  def removeCheckpoint(soeHash: String): F[Unit] =
    accepted.remove(soeHash) >>
      waitingForAcceptance.remove(soeHash) >>
      tips.remove(soeHash) >>
      usages(soeHash).set(none) >>
      checkpoints(soeHash).set(none) >>
      removeInSnapshot(soeHash)

  def isCheckpointInAcceptance(soeHash: String): F[Boolean] =
    inAcceptance.exists(soeHash)

  def isCheckpointWaitingForAcceptance(soeHash: String): F[Boolean] =
    waitingForAcceptance.exists(soeHash)

  def isCheckpointAwaiting(soeHash: String): F[Boolean] =
    awaiting.exists(soeHash)

  def acceptCheckpoint(soeHash: String): F[Unit] =
    accepted.add(soeHash) >>
      waitingForAcceptance.remove(soeHash) >>
      inAcceptance.remove(soeHash) >>
      awaiting.remove(soeHash) >>
      waitingForResolving.remove(soeHash) >>
      unmarkForAcceptanceAfterDownload(soeHash)

  def existsCheckpoint(soeHash: String): F[Boolean] =
    checkpoints(soeHash).get.map(_.nonEmpty)

  def markWaitingForAcceptance(soeHash: String): F[Unit] =
    waitingForAcceptance.add(soeHash)

  def markForAcceptance(soeHash: String): F[Unit] =
    inAcceptance.add(soeHash) >>
      waitingForAcceptance.remove(soeHash)

  def unmarkFromAcceptance(soeHash: String): F[Unit] =
    inAcceptance.remove(soeHash)

  def getWaitingForAcceptance: F[Set[CheckpointCache]] =
    waitingForAcceptance.get.flatMap {
      _.toList.traverse {
        getCheckpoint
      }
    }.map(_.flatten).map(_.toSet)

  def setWaitingForAcceptance(soeHashes: Set[String]): F[Unit] =
    waitingForAcceptance.set(soeHashes)

  def unmarkWaitingForAcceptance(soeHash: String): F[Unit] =
    waitingForAcceptance.remove(soeHash)

  def markForAcceptanceAfterDownload(cb: CheckpointCache): F[Unit] =
    waitingForAcceptanceAfterDownload.add(cb)

  def getCheckpointsForAcceptanceAfterDownload: F[Set[CheckpointCache]] =
    waitingForAcceptanceAfterDownload.modify { cbs =>
      (Set.empty, cbs)
    }

  def unmarkForAcceptanceAfterDownload(soeHash: String): F[Unit] =
    waitingForAcceptanceAfterDownload.modify { cbs =>
      (cbs.filterNot(_.checkpointBlock.soeHash == soeHash), ())
    }

  def isCheckpointWaitingForAcceptanceAfterDownload(soeHash: String): F[Boolean] =
    waitingForAcceptanceAfterDownload.get.map(_.exists(_.checkpointBlock.soeHash == soeHash))

  def getAccepted: F[Set[String]] =
    accepted.get

  def setAccepted(soeHashes: Set[String]): F[Unit] =
    accepted.set(soeHashes)

  def isInSnapshot(soeHash: String): F[Boolean] =
    inSnapshot.get.map(_.map(_._1).contains(soeHash))

  def markInSnapshot(soeHash: String, height: Long): F[Unit] =
    inSnapshot.add((soeHash, height))

  def markInSnapshot(soeHashes: Set[(String, Long)]): F[Unit] =
    inSnapshot.modify { s =>
      (s ++ soeHashes, ())
    }

  def removeInSnapshot(soeHash: String): F[Unit] =
    inSnapshot.modify { s =>
      (s.filterNot(_._1 == soeHash), ())
    }

  def setInSnapshot(soeHashes: Set[(String, Long)]): F[Unit] =
    inSnapshot.set(soeHashes)

  def getInSnapshot: F[Set[(String, Long)]] =
    inSnapshot.get

  def areParentsAccepted(checkpoint: CheckpointCache): F[Boolean] =
    checkpoint.checkpointBlock.parentSOEHashes.distinct.toList
      .filterNot(_.equals(Genesis.Coinbase))
      .traverse { isCheckpointAccepted }
      .map(_.forall(_ == true))

  def areParentsAccepted(checkpoint: CheckpointCache, isAccepted: String => Boolean): Boolean =
    checkpoint.checkpointBlock.parentSOEHashes.distinct.toList
      .filterNot(_.equals(Genesis.Coinbase))
      .map { isAccepted }
      .forall(_ == true)

  def isCheckpointAccepted(soeHash: String): F[Boolean] =
    accepted
      .exists(soeHash)
      .ifM(
        F.pure(true),
        isInSnapshot(soeHash)
      )

  def markAsAwaiting(soeHash: String): F[Unit] =
    awaiting.add(soeHash)

  def getAwaiting: F[Set[String]] = awaiting.get

  def setAwaiting(a: Set[String]): F[Unit] =
    awaiting.set(a)

  def calculateHeight(soeHash: String): F[Option[Height]] =
    for {
      checkpointHeight <- getCheckpoint(soeHash).map(_.map(_.height))
      calculatedHeight <- getParents(soeHash).map {
        _.flatMap { parents =>
          val maybeHeight = parents.map(_.height)
          if (maybeHeight.isEmpty)
            none[Height]
          else
            Height(maybeHeight.map(_.min).min + 1, maybeHeight.map(_.max).max + 1).some
        }
      }
    } yield checkpointHeight.orElse(calculatedHeight)

  def calculateHeight(checkpoint: CheckpointBlock): F[Option[Height]] =
    checkpoint.parentSOEHashes.toList.traverse { getCheckpoint }
      .map(a => a.flatten)
      .map { parents =>
        val maybeHeight = parents.map(_.height)
        if (maybeHeight.isEmpty)
          none[Height]
        else
          Height(maybeHeight.map(_.min).min + 1, maybeHeight.map(_.max).max + 1).some
      }

  def getParents(soeHash: String): F[Option[List[CheckpointCache]]] =
    getParentSoeHashes(soeHash)
      .flatMap(_.traverse(_.traverse(getCheckpoint)))
      .map(_.map(_.flatten))

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
    getTips.map(_.size)

  def getMinTipHeight: F[Long] =
    for {
      tips <- getTips
      minHeights = tips.map(_._2.min)
      minHeight = if (tips.nonEmpty) minHeights.min else 0L
    } yield minHeight

  def getMinWaitingHeight: F[Option[Long]] =
    for {
      waiting <- getWaitingForAcceptance
      minHeights = waiting.map(_.height.min)
    } yield if (minHeights.nonEmpty) minHeights.min.some else none

  def getTips: F[Set[(String, Height)]] =
    tips.get
      .flatMap(_.toList.traverse(getCheckpoint))
      .map(
        a =>
          a.flatten.map { checkpoint =>
            (checkpoint.checkpointBlock.soeHash, checkpoint.height)
          }
      )
      .map(_.toSet)

  def getCheckpoint(soeHash: String): F[Option[CheckpointCache]] =
    checkpoints(soeHash).get

  def getCheckpoints: F[Map[String, CheckpointCache]] =
    checkpoints.toMap

  def setCheckpoints(map: Map[String, CheckpointCache]): F[Unit] =
    checkpoints.clear >> map.toList.traverse {
      case (soeHash, checkpoint) => checkpoints(soeHash).set(checkpoint.some)
    }.void
}
