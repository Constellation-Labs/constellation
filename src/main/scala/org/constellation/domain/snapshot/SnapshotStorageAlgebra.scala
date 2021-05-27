package org.constellation.domain.snapshot

import org.constellation.schema.snapshot.StoredSnapshot

trait SnapshotStorageAlgebra[F[_]] {

  def getStoredSnapshot: F[StoredSnapshot]
  def setStoredSnapshot(snapshot: StoredSnapshot): F[Unit]

  def getLastSnapshotHeight: F[Int]
  def setLastSnapshotHeight(height: Int): F[Unit]

  def getAcceptedCheckpointsSinceSnapshot: F[Set[String]]
  def isCheckpointInAcceptedSinceSnapshot(soeHash: String): F[Boolean]
  def countAcceptedCheckpointsSinceSnapshot: F[Int]
  def addAcceptedCheckpointSinceSnapshot(soeHash: String): F[Unit]
  def filterOutAcceptedCheckpointsSinceSnapshot(hashes: Set[String]): F[Unit]
  def setAcceptedCheckpointsSinceSnapshot(hashes: Set[String]): F[Unit]

  def getNextSnapshotHash: F[String]
  def setNextSnapshotHash(hash: String): F[Unit]

  def exists(hash: String): F[Boolean]

}
