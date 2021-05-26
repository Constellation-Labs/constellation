package org.constellation.infrastructure.snapshot

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.schema.snapshot.Snapshot.snapshotZero
import org.constellation.schema.snapshot.StoredSnapshot
import org.constellation.concurrency.SetRefUtils.RefOps

class SnapshotStorageInterpreter[F[_]](
                                        snapshotStorage: LocalFileStorage[F, StoredSnapshot]
                                      )(implicit F: Sync[F]) extends SnapshotStorageAlgebra[F] {

  val acceptedCheckpointsSinceSnapshot: Ref[F, Set[String]] = Ref.unsafe(Set.empty)
  val storedSnapshot: Ref[F, StoredSnapshot] = Ref.unsafe(StoredSnapshot(snapshotZero, Seq.empty))

  val totalCheckpointsInSnapshots: Ref[F, Long] = Ref.unsafe(0L)
  val lastSnapshotHeight: Ref[F, Int] = Ref.unsafe(0L)
  val nextSnapshotHash: Ref[F, String] = Ref.unsafe("")

  def getStoredSnapshot: F[StoredSnapshot] =
    storedSnapshot.get

  def setStoredSnapshot(snapshot: StoredSnapshot): F[Unit] =
    storedSnapshot.set(snapshot)

  def exists(hash: String): F[Boolean] =
    getStoredSnapshot.flatMap { stored =>
      F.pure(stored.snapshot.hash == hash).ifM(
        F.pure(true),
        isStored(hash)
      )
    }

  def isStored(hash: String): F[Boolean] =
    snapshotStorage.exists(hash)

  def getLastSnapshotHeight: F[Int] =
    lastSnapshotHeight.get

  def setLastSnapshotHeight(height: Int): F[Unit] =
    lastSnapshotHeight.set(height)

  def getAcceptedCheckpointsSinceSnapshot: F[Set[String]] =
    acceptedCheckpointsSinceSnapshot.get

  def countAcceptedCheckpointsSinceSnapshot: F[Int] =
    getAcceptedCheckpointsSinceSnapshot.map(_.size)

  def addAcceptedCheckpointSinceSnapshot(soeHash: String): F[Unit] =
    acceptedCheckpointsSinceSnapshot.add(soeHash)

  def filterOutAcceptedCheckpointsSinceSnapshot(hashes: Set[String]): F[Unit] =
    acceptedCheckpointsSinceSnapshot.update(_.diff(hashes))

  def setAcceptedCheckpointsSinceSnapshot(hashes: Set[String]): F[Unit] =
    acceptedCheckpointsSinceSnapshot.set(hashes)

  def getNextSnapshotHash: F[String] =
    nextSnapshotHash.get

  def setNextSnapshotHash(hash: String): F[Unit] =
    nextSnapshotHash.set(hash)

}
