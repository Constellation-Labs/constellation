package org.constellation.domain.snapshot

import better.files.File
import cats.data.EitherT

trait SnapshotInfoStorage[F[_]] {
  def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit]

  def exists(hash: String): F[Boolean]

  def readSnapshotInfo(hash: String): EitherT[F, Throwable, SnapshotInfo]

  def getSnapshotInfoBytes(hash: String): EitherT[F, Throwable, Array[Byte]]

  def writeSnapshotInfo(hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit]

  def removeSnapshotInfo(hash: String): EitherT[F, Throwable, Unit]

  def getSnapshotInfoHashes: F[List[String]]

  def getSnapshotInfoFiles: F[List[File]]

  def getSnapshotInfoFiles(hashes: List[String]): F[List[File]]
}
