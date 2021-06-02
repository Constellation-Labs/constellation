package org.constellation.domain.snapshot

import org.constellation.schema.snapshot.StoredSnapshot

trait SnapshotStorageAlgebra[F[_]] {

  def getStoredSnapshot: F[StoredSnapshot]
  def setStoredSnapshot(snapshot: StoredSnapshot): F[Unit]

  def getLastSnapshotHeight: F[Int]
  def setLastSnapshotHeight(height: Int): F[Unit]

  def getNextSnapshotHash: F[String]
  def setNextSnapshotHash(hash: String): F[Unit]

  def exists(hash: String): F[Boolean]

}
