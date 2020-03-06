package org.constellation.infrastructure.cloud

import java.io.FileInputStream

import better.files.File
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.{Blob, Bucket, Storage, StorageOptions}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.cloud.CloudStorage.StorageName
import org.constellation.domain.cloud.CloudStorage.StorageName.StorageName

import scala.util.{Failure, Success, Try}

class GCPStorage[F[_]: Concurrent](bucketName: String, pathToPermissionFile: String) extends CloudStorage[F] {

  private val logger = Slf4jLogger.getLogger[F]

  def upload(files: Seq[File], storageName: StorageName): F[List[String]] = {
    val upload = for {
      credentials <- createGoogleCredentials()
      _ <- logger.debug("[CloudStorage] Credentials created successfully")

      service <- getStorage(credentials)
      _ <- logger.debug("[CloudStorage] Service created successfully")

      bucket <- getBucket(service)
      _ <- logger.debug("[CloudStorage] Bucket got successfully")

      blobs <- saveToBucket(bucket, files, StorageName.toDirectory(storageName))
      _ <- logger.debug("[CloudStorage] Files saved successfully")
    } yield blobs.map(b => b.getName)

    upload.handleErrorWith(
      err =>
        logger.error(s"[CloudStorage] Cannot upload files : GCP : ${err.getMessage}") >> Sync[F].pure(
          List.empty[String]
        )
    )
  }

  def get(filename: String, storageName: StorageName): F[Array[Byte]] =
    for {
      credentials <- createGoogleCredentials()
      service <- getStorage(credentials)
      bucket <- getBucket(service)
      blob <- getFromBucket(bucket, filename, StorageName.toDirectory(storageName))
    } yield blob

  private def saveToBucket(bucket: Bucket, files: Seq[File], dir: String): F[List[Blob]] =
    files.toList.traverse(file => uploadFile(bucket, dir, file))

  private def getFromBucket(bucket: Bucket, filename: String, dir: String): F[Array[Byte]] =
    for {
      blob <- Sync[F].delay { Try(bucket.get(s"$dir/${filename}")) }.flatMap {
        case Success(blob)      => blob.pure[F]
        case Failure(exception) => CannotGetFile(exception).raiseError[F, Blob]
      }
      bytes <- Sync[F].delay { blob.getContent() }
    } yield bytes

  private def uploadFile(bucket: Bucket, dir: String, file: File): F[Blob] =
    Sync[F].delay(Try(bucket.create(s"$dir/${file.name}", file.byteArray))).flatMap {
      case Success(blob)      => Sync[F].pure(blob)
      case Failure(exception) => CannotUploadFile(exception).raiseError[F, Blob]
    }

  private def createGoogleCredentials(): F[GoogleCredentials] =
    Sync[F].delay(GoogleCredentials.fromStream(new FileInputStream(pathToPermissionFile)))

  private def getStorage(credentials: GoogleCredentials): F[Storage] =
    buildConfigStorage(credentials).flatMap {
      case Success(storage)   => Sync[F].pure(storage)
      case Failure(exception) => CannotGetService(exception).raiseError[F, Storage]
    }

  private def buildConfigStorage(credentials: GoogleCredentials): F[Try[Storage]] =
    Sync[F].delay(Try(StorageOptions.newBuilder().setCredentials(credentials).build().getService))

  private def getBucket(service: Storage): F[Bucket] =
    Sync[F].pure(service.get(bucketName))
}
