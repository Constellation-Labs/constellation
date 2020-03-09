package org.constellation.domain.rewards

import better.files.File
import cats.data.EitherT

trait EigenTrustStorage[F[_]] {

  def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit]

  def exists(hash: String): F[Boolean]

  def readEigenTrust(hash: String): EitherT[F, Throwable, StoredEigenTrust]

  def writeEigenTrust(hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit]

  def removeEigenTrust(hash: String): EitherT[F, Throwable, Unit]

  def getUsableSpace: F[Long]

  def getOccupiedSpace: F[Long]

  def getEigenTrustHashes: F[List[String]]

  def getEigenTrustFiles: F[List[File]]

  def getEigenTrustFiles(hashes: List[String]): F[List[File]]

  def getEigenTrustBytes(hash: String): EitherT[F, Throwable, Array[Byte]]
}
