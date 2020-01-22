package org.constellation.rollback

import better.files.File
import org.constellation.consensus.{SnapshotInfo, SnapshotInfoSer, StoredSnapshot}
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
    Try(loadSnapshotInfoSer(File(snapshotInfoPath).pathAsString))
      .map(Right(_))
      .getOrElse(Left(CannotLoadSnapshotInfoFile(snapshotInfoPath)))

  def loadGenesisObservation(): Either[RollbackException, GenesisObservation] =
    Try(deserializeFromFile[GenesisObservation](genesisObservationPath))
      .map(Right(_))
      .getOrElse(Left(CannotLoadGenesisObservationFile(genesisObservationPath)))

  def loadSnapshotInfoSer(snapshotInfoDir: String): SnapshotInfo = {
    val snapInfoSerParts = File(snapshotInfoDir)
      .glob("**")
      .map { file =>
        val Array(dataType, partId) = file.pathAsString.split("-")
        val loadedPartFile = File(file.pathAsString).byteArray
        (dataType.split("/").last, (partId, loadedPartFile))
      }
      .toSeq
      .groupBy(_._1)
      .mapValues { v =>
        v.map(_._2).sortBy(_._1.toInt).map(_._2).toArray
      }
    val serSnapInfo = SnapshotInfoSer(
      snapInfoSerParts.getOrElse("snapshot", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("snapshotCheckpointBlocks", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("acceptedCBSinceSnapshot", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("acceptedCBSinceSnapshotCache", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("acceptedCbs", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("lastSnapshotHeight", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("snapshotHashes", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("addressCacheData", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("tips", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("snapshotCache", Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse("lastAcceptedTransactionRef", Array.empty[Array[Byte]])
    )
    serSnapInfo.toSnapshotInfo()
  }

  private def deserializeFromFile[T](path: String): T =
    KryoSerializer.deserializeCast[T](File(path).byteArray)

  private def deserializeAllFromDirectory[T](directory: String): Seq[T] =
    getListFilesFromDirectory(directory).map(s => deserializeFromFile[T](s))

  private def getListFilesFromDirectory(directory: String): Seq[File] =
    File(directory).list.toSeq

  private def deserializeFromFile[T](file: File): T =
    KryoSerializer.deserializeCast(file.byteArray)
}
