package org.constellation.rollback

import better.files.File
import org.constellation.consensus.{SnapshotInfo, SnapshotInfoSer, StoredSnapshot}
import org.constellation.domain.snapshotInfo.SnapshotInfoChunk
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
        val Array(dataType, partId) = file.name.split('-')
        val loadedPartFile = file.byteArray
        (dataType.split("/").last, (partId, loadedPartFile))
      }
      .toSeq
      .groupBy(_._1)
      .mapValues { v =>
        v.map(_._2).sortBy(_._1.toInt).map(_._2).toArray
      }
    val serSnapInfo = SnapshotInfoSer(
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.SNAPSHOT.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.CHECKPOINT_BLOCKS.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.ACCEPTED_CBS_SINCE_SNAPSHOT.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.ACCEPTED_CBS_SINCE_SNAPSHOT_CACHE.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.AWAITING_CBS.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.LAST_SNAPSHOT_HEIGHT.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.SNAPSHOT_HASHES.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.ADDRESS_CACHE_DATA.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.TIPS.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.SNAPSHOT_CACHE.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.LAST_ACCEPTED_TX_REF.name, Array.empty[Array[Byte]]),
      snapInfoSerParts.getOrElse(SnapshotInfoChunk.PUBLIC_REPUTATION.name, Array.empty[Array[Byte]])
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
