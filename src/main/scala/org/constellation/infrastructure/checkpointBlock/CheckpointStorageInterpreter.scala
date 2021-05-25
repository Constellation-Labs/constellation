package org.constellation.infrastructure.checkpointBlock

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap
import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.{Ref, Semaphore}
import cats.syntax.all._
import io.chrisdavenport.mapref.MapRef
import org.constellation.{ConstellationExecutionContext, PendingAcceptance}
import org.constellation.checkpoint.CheckpointMerkleService
import org.constellation.concurrency.MapRefUtils
import org.constellation.concurrency.SetRefUtils.RefOps
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.genesis.Genesis
import org.constellation.schema.Height
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, CheckpointCacheMetadata}
import org.constellation.schema.edge.SignedObservationEdge
import org.constellation.storage.algebra.Lookup
import org.constellation.storage.ConcurrentStorageService

class CheckpointStorageInterpreter[F[_]](merkleService: CheckpointMerkleService[F])(implicit F: Concurrent[F])
    extends CheckpointStorageAlgebra[F] {

  val checkpoints: MapRef[F, String, Option[CheckpointCache]] = MapRefUtils.ofConcurrentHashMap() // Consider cache and time-removal

  val waitingForAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val inAcceptance: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val accepted: Ref[F, Set[String]] = Ref.unsafe(Set.empty)

  val awaiting: Ref[F, Set[String]] = Ref.unsafe(Set())

  def persistCheckpoint(checkpoint: CheckpointCache): F[Unit] =
    checkpoints(checkpoint.checkpointBlock.soeHash).set(checkpoint.some)

  def getCheckpoint(soeHash: String): F[Option[CheckpointCache]] =
    checkpoints(soeHash).get

  def updateCheckpointHeight(soeHash: String, height: Option[Height]): F[Unit] =
    checkpoints(soeHash).update(_.map { cb =>
      cb.copy(height = height)
    })

  def removeCheckpoint(soeHash: String): F[Unit] =
    checkpoints(soeHash).set(none) >> accepted.remove(soeHash)

  def removeCheckpoints(soeHashes: List[String]): F[Unit] =
    soeHashes.traverse(removeCheckpoint).void

  def isCheckpointAccepted(soeHash: String): F[Boolean] =
    accepted.exists(soeHash)

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

  def existsCheckpoint(soeHash: String): F[Boolean] =
    checkpoints(soeHash).get.map(_.nonEmpty)

  def markForAcceptance(soeHash: String): F[Unit] =
    inAcceptance.add(soeHash) >>
      waitingForAcceptance.remove(soeHash)

  def unmarkFromAcceptance(soeHash: String): F[Unit] =
    inAcceptance.remove(soeHash)

  def areParentsAccepted(checkpoint: CheckpointCache): F[Boolean] =
    checkpoint.checkpointBlock
      .parentSOEHashes
      .distinct
      .toList
      .traverse { isCheckpointAccepted }
      .map(_.forall(_ == true))


  def markAsAwaiting(soeHash: String): F[Unit] =
    awaiting.add(soeHash)

  def getAwaiting: F[Set[String]] = awaiting.get

  def getParentSoeHashes(soeHash: String): F[Option[List[String]]] =
    getCheckpoint(soeHash).nested.map(_.checkpointBlock.parentSOEHashes.toList).value

  def getParents(soeHash: String): F[Option[List[CheckpointCache]]] =
    getParentSoeHashes(soeHash)
      .flatMap(_.traverse(_.traverse(getCheckpoint)))
      .map(_.flatSequence)

  def calculateHeight(soeHash: String): F[Option[Height]] =
    getParents(soeHash).map { _.flatMap { parents =>
      val maybeHeight = parents.flatMap(_.height)
      if (maybeHeight.isEmpty)
        none[Height]
      else
        Height(maybeHeight.map(_.min).min + 1, maybeHeight.map(_.max).max + 1).some
    }}

  // *** //


//  val checkpointSemaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()
//  val checkpointMemPool =
//    new ConcurrentStorageService[F, CheckpointCacheMetadata](checkpointSemaphore, "CheckpointMemPool".some)

//  val soeSemaphore: Semaphore[F] = ConstellationExecutionContext.createSemaphore()
//  val soeMemPool = new ConcurrentStorageService[F, SignedObservationEdge](soeSemaphore, "SoeMemPool".some)

  val checkpointsWaitingForResolving: Ref[F, Set[String]] = Ref.unsafe(Set())

//
//  def getParentSoeHashesDirect(checkpoint: CheckpointBlock): List[String] =
//    checkpoint.parentSOEHashes.toList.traverse { soeHash =>
//      if (soeHash.equals(Genesis.Coinbase)) {
//        none[String]
//      } else {
//        checkpoint.checkpoint.edge.observationEdge.parents.find(_.hashReference == soeHash).flatMap(_.baseHash)
//      }
//    }.getOrElse(List.empty)

//
//  def getParentSOESoeHashes(checkpoint: CheckpointBlock): F[List[String]] =
//    checkpoint.parentSOEHashes.toList.traverse { soeHash =>
//      if (soeHash.equals(Genesis.Coinbase)) {
//        F.pure(none[String])
//      } else {
//        lookupSoe(soeHash).map { parent =>
//          if (parent.isEmpty) {
//            checkpoint.checkpoint.edge.observationEdge.parents.find(_.hashReference == soeHash).flatMap { _.baseHash }
//          } else parent.map(_.baseHash)
//        }
//      }
//    }.map(_.flatten)

  def markCheckpointForResolving(soeHash: String): F[Unit] =
    checkpointsWaitingForResolving.add(soeHash)

  def unmarkCheckpointForResolving(soeHash: String): F[Unit] =
    checkpointsWaitingForResolving.remove(soeHash)

}
