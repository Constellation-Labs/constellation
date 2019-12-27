package org.constellation.genesis

import better.files.File
import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.external.CloudStorage
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.ExecutionContext
import scala.util.Try

class GenesisObservationWriter[F[_]: Concurrent](
  cloudStorage: CloudStorage[F],
  dao: DAO,
  executionContext: ContextShift[F],
  fileOperationContext: ExecutionContext
) {

  private val logger = Slf4jLogger.getLogger[F]

  def write(genesisObservation: GenesisObservation): EitherT[F, GenesisObservationWriterException, Unit] =
    for {
      _ <- EitherT(writeToDisk(genesisObservation))
      _ <- EitherT.liftF(logger.info("Genesis observation saved on disk"))

      isEnabledCloudStorage = ConfigUtil.isEnabledCloudStorage
      _ <- EitherT.liftF(logger.info(s"Cloud storage is enabled : $isEnabledCloudStorage"))

      result <- sendToCloudIfRequired(genesisObservation)
      _ <- EitherT.liftF(logger.info(s"Genesis observation files saved on cloud : ${result.size} : ${result}"))
    } yield ()

  private def writeToDisk(genesis: GenesisObservation): F[Either[GenesisObservationWriterException, File]] =
    executionContext.evalOn(fileOperationContext) {
      Sync[F].delay {
        Try(getFilePathToGenesisObservation.writeByteArray(KryoSerializer.serializeAnyRef(genesis))).toEither
          .leftMap(CannotWriteGenesisObservationToDisk(_))
      }
    }

  private def sendToCloudIfRequired(
    genesis: GenesisObservation
  ): EitherT[F, GenesisObservationWriterException, List[String]] =
    if (ConfigUtil.isEnabledCloudStorage) EitherT(sendToCloud(genesis)) else EitherT.rightT(List.empty)

  private def sendToCloud(genesis: GenesisObservation): F[Either[GenesisObservationWriterException, List[String]]] =
    executionContext.evalOn(fileOperationContext) {
      cloudStorage.upload(List(getFilePathToGenesisObservation)).map {
        case Nil  => Left(CannotSendGenesisObservationToCloud("List is empty"))
        case list => Right(list)
      }
    }

  private def getFilePathToGenesisObservation: File =
    File(GenesisObservationWriterProperties.path(dao.id), GenesisObservationWriterProperties.FILE_NAME)
}

object GenesisObservationWriterProperties {

  final val FILE_NAME = "genesisObservation"

  def path(id: Id): File =
    File(s"tmp/${id.medium}/genesis").createDirectoryIfNotExists()
}
