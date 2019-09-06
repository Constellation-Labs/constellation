package org.constellation.storage.external

import java.io.FileInputStream

import better.files.File
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage._
import io.chrisdavenport.log4cats.Logger
import org.constellation.ConfigUtil

import scala.util.{Failure, Success, Try}

sealed trait CloudStorage[F[_]] {

  def upload(files: Seq[File]): F[List[String]]
}

class GcpStorage[F[_]: Concurrent: Logger] extends CloudStorage[F] {

  override def upload(files: Seq[File]): F[List[String]] = {
    val upload = for {
      credentials <- createCredentials()
      _ <- Logger[F].debug("Credentials created successfully")

      service <- getStorage(credentials)
      _ <- Logger[F].debug("Service created successfully")

      bucket <- getBucket(service)
      _ <- Logger[F].debug("Bucket got successfully")

      blobs <- saveToBucket(bucket, files)
      _ <- Logger[F].debug("Files saved successfully")
    } yield blobs.map(b => b.getName)

    upload.handleErrorWith(
      err => Logger[F].error(s"Cannot upload files : ${err.getMessage}") *> Sync[F].pure(List.empty[String])
    )
  }

  private def saveToBucket(bucket: Bucket, files: Seq[File]): F[List[Blob]] =
    files.toList.traverse(file => uploadFile(bucket, file))

  private def uploadFile(bucket: Bucket, file: File): F[Blob] =
    Sync[F].delay(Try(bucket.create(file.name, file.byteArray))).flatMap {
      case Success(blob)      => Sync[F].pure(blob)
      case Failure(exception) => CannotUploadFile(exception).raiseError[F, Blob]
    }

  private def createCredentials(): F[GoogleCredentials] =
    getConfig("constellation.storage.gcp.path-to-permission-file").flatMap {
      case Success(filePath)  => createGoogleCredentials(filePath)
      case Failure(exception) => CannotGetConfigProperty(exception).raiseError[F, GoogleCredentials]
    }

  private def createGoogleCredentials(filePath: String): F[GoogleCredentials] =
    Sync[F].delay(GoogleCredentials.fromStream(new FileInputStream(filePath)))

  private def getStorage(credentials: GoogleCredentials): F[Storage] =
    buildConfigStorage(credentials).flatMap {
      case Success(storage)   => Sync[F].pure(storage)
      case Failure(exception) => CannotGetService(exception).raiseError[F, Storage]
    }

  private def buildConfigStorage(credentials: GoogleCredentials): F[Try[Storage]] =
    Sync[F].delay(Try(StorageOptions.newBuilder().setCredentials(credentials).build().getService))

  private def getBucket(service: Storage): F[Bucket] =
    getConfig("constellation.storage.gcp.bucket-name").flatMap {
      case Success(bucketName) => Sync[F].pure(service.get(bucketName))
      case Failure(exception)  => CannotGetBucket(exception).raiseError[F, Bucket]
    }

  private def getConfig(configName: String): F[Try[String]] =
    Sync[F].delay(ConfigUtil.get(configName))
}

class AwsStorage[F[_]: Concurrent: Logger] extends CloudStorage[F] {

  override def upload(files: Seq[File]): F[List[String]] = ???
}
