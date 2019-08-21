package org.constellation.rollback

import better.files.File
import cats.data.EitherT
import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.{SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.serializer.KryoSerializer
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class RollbackLoader[F[_]: Concurrent](
  rollbackDataDirectory: String = "rollback_data",
  snapshotDataDirectory: String = "snapshots",
  snapshotInfoFile: String = "rollback_info",
  genesisObservationFile: String = "rollback_genesis"
) {

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def loadSnapshotsFromFile(): EitherT[F, RollbackException, Seq[StoredSnapshot]] = EitherT.fromEither[F] {
    Try {
      File(rollbackDataDirectory + "/" + snapshotDataDirectory).list.toSeq
        .map(s => KryoSerializer.deserializeCast[StoredSnapshot](s.byteArray))
    } match {
      case Success(value) =>
        logger.info(s"Snapshots loaded")
        Right(value)
      case Failure(exception) =>
        logger.error(s"Cannot load snapshots : ${exception.getMessage}")
        Left(CannotLoadSnapshots)
    }
  }

  def loadSnapshotInfoFromFile(): EitherT[F, RollbackException, SnapshotInfo] = EitherT.fromEither[F] {
    Try {
      KryoSerializer.deserializeCast[SnapshotInfo](File(rollbackDataDirectory, snapshotInfoFile).byteArray)
    } match {
      case Success(value) =>
        logger.info(s"Snapshot Info loaded")
        Right(value)
      case Failure(exception) =>
        logger.error(s"Cannot load snapshot info : ${exception.getMessage}")
        Left(CannotLoadSnapshotInfoFile)
    }
  }

  def loadGenesisObservation(): EitherT[F, RollbackException, GenesisObservation] = EitherT.fromEither[F] {
    Try {
      KryoSerializer.deserializeCast[GenesisObservation](File(rollbackDataDirectory, genesisObservationFile).byteArray)
    } match {
      case Success(value) =>
        logger.info(s"Genesis Observation loaded")
        Right(value)
      case Failure(exception) =>
        logger.error(s"Cannot load genesis observation : ${exception.getMessage}")
        Left(CannotLoadGenesisObservationFile)
    }
  }
}
