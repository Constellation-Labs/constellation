package org.constellation.rollback

import better.files.File
import org.constellation.consensus.{SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.serializer.KryoSerializer

import scala.util.Try

class RollbackLoader(
  rollbackDataDirectory: String = "rollback_data",
  snapshotDataDirectory: String = "snapshots",
  snapshotInfoFile: String = "rollback_info",
  genesisObservationFile: String = "rollback_genesis"
) {

  def loadSnapshotsFromFile(): Either[RollbackException, Seq[StoredSnapshot]] =
    Try(deserializeAllFromDirectory[StoredSnapshot](s"$rollbackDataDirectory/$snapshotDataDirectory"))
      .map(Right(_))
      .getOrElse(Left(CannotLoadSnapshotsFiles))

  def loadSnapshotInfoFromFile(): Either[RollbackException, SnapshotInfo] =
    Try(deserializeFromFile[SnapshotInfo](rollbackDataDirectory, snapshotInfoFile))
      .map(Right(_))
      .getOrElse(Left(CannotLoadSnapshotInfoFile))

  def loadGenesisObservation(): Either[RollbackException, GenesisObservation] =
    Try(deserializeFromFile[GenesisObservation](rollbackDataDirectory, genesisObservationFile))
      .map(Right(_))
      .getOrElse(Left(CannotLoadGenesisObservationFile))

  private def deserializeAllFromDirectory[T](directory: String): Seq[T] =
    getListFilesFromDirectory(directory).map(s => deserializeFromFile[T](s))

  private def getListFilesFromDirectory(directory: String): Seq[File] =
    File(directory).list.toSeq

  private def deserializeFromFile[T](file: File): T =
    KryoSerializer.deserializeCast(file.byteArray)

  private def deserializeFromFile[T](directory: String, fileName: String): T =
    KryoSerializer.deserializeCast[T](File(directory, fileName).byteArray)
}
