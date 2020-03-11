package org.constellation.rollback

import better.files.File
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.serializer.KryoSerializer

import scala.util.Try

class RollbackLoader(
  snapshotsPath: String,
  snapshotInfoPath: String,
  genesisObservationPath: String
) {

  def loadSnapshotsFromFile(): Either[RollbackException, Seq[StoredSnapshot]] =
    Try(deserializeAllFromDirectory[StoredSnapshot](snapshotsPath))
      .map(Right(_))
      .getOrElse(Left(CannotLoadSnapshotsFiles(snapshotsPath)))

  def loadSnapshotInfoFromFile(): Either[RollbackException, SnapshotInfo] =
    Try(deserializeFromFile[SnapshotInfo](snapshotInfoPath)).toEither
      .map(Right(_))
      .getOrElse(Left(CannotLoadSnapshotInfoFile(snapshotInfoPath)))

  def loadGenesisObservation(): Either[RollbackException, GenesisObservation] =
    Try(deserializeFromFile[GenesisObservation](genesisObservationPath))
      .map(Right(_))
      .getOrElse(Left(CannotLoadGenesisObservationFile(genesisObservationPath)))

  private def deserializeFromFile[T](path: String): T =
    KryoSerializer.deserializeCast[T](File(path).byteArray)

  private def deserializeAllFromDirectory[T](directory: String): Seq[T] =
    getListFilesFromDirectory(directory).map(s => deserializeFromFile[T](s))

  private def getListFilesFromDirectory(directory: String): Seq[File] =
    File(directory).list.toSeq

  private def deserializeFromFile[T](file: File): T =
    KryoSerializer.deserializeCast(file.byteArray)
}
