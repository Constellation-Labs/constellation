package org.constellation.domain.snapshot

import better.files.File
import cats.data.EitherT
import org.constellation.consensus.StoredSnapshot

trait SnapshotStorage[F[_]] {

  def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit]

  def exists(hash: String): F[Boolean]

  def readSnapshot(hash: String): EitherT[F, Throwable, StoredSnapshot]

  def writeSnapshot(hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit]

  def removeSnapshot(hash: String): EitherT[F, Throwable, Unit]

  def getUsableSpace: F[Long]

  def getOccupiedSpace: F[Long]

  def getSnapshotHashes: F[Iterator[String]]

  def getSnapshotFiles: F[Iterator[File]]

  def getSnapshotFiles(hashes: List[String]): F[Iterator[File]]

  def getSnapshotBytes(hash: String): EitherT[F, Throwable, Array[Byte]]
}
