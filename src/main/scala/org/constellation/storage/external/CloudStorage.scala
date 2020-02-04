package org.constellation.storage.external

import java.io.FileInputStream

import better.files.File
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.{PutObjectRequest, PutObjectResult}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder}
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConfigUtil
import org.constellation.alerts.AlertClient
import org.constellation.alerts.primitives.DataMigrationAlert

import scala.util.{Failure, Success, Try}

sealed trait CloudStorage[F[_]] {

  def upload(files: Seq[File]): F[List[String]]
}

class GCPStorage[F[_]: Concurrent](alertClient: AlertClient[F]) extends CloudStorage[F] {

  private val logger = Slf4jLogger.getLogger[F]

  override def upload(files: Seq[File]): F[List[String]] = {
    val upload = for {
      credentials <- createCredentials()
      _ <- logger.debug("[CloudStorage] Credentials created successfully")

      service <- getStorage(credentials)
      _ <- logger.debug("[CloudStorage] Service created successfully")

      bucket <- getBucket(service)
      _ <- logger.debug("[CloudStorage] Bucket got successfully")

      blobs <- saveToBucket(bucket, files)
      _ <- logger.debug("[CloudStorage] Files saved successfully")
    } yield blobs.map(b => b.getName)

    upload.handleErrorWith(
      err =>
        logger.error(s"[CloudStorage] Cannot upload files : GCP : ${err.getMessage}") >> alertClient.sendAlert(
          DataMigrationAlert()
        ) >> Sync[F].pure(List.empty[String])
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

class AWSStorage[F[_]: Concurrent](alertClient: AlertClient[F]) extends CloudStorage[F] {

  private val logger = Slf4jLogger.getLogger[F]

  override def upload(files: Seq[File]): F[List[String]] = {
    val upload = for {
      accessKey <- getAccessKey
      secretKey <- getSecretKey
      region <- getRegion
      service <- getService(accessKey, secretKey, region)
      _ <- logger.debug("[CloudStorage] Service created successfully")

      bucketName <- getBucketName
      _ <- checkBucket(service, bucketName)
      _ <- logger.debug("[CloudStorage] Bucket got successfully")

      result <- saveToBucket(service, bucketName, files)
      _ <- logger.debug("[CloudStorage] Files saved successfully")
    } yield result.map(_._1)

    upload.handleErrorWith(
      err =>
        logger.error(s"[CloudStorage] Cannot upload files : AWS : ${err.getMessage}") >> alertClient.sendAlert(
          DataMigrationAlert()
        ) >> Sync[F].pure(List.empty[String])
    )
  }

  private def saveToBucket(service: AmazonS3, bucketName: String, files: Seq[File]) =
    files.toList.traverse(file => uploadFile(service, bucketName, file.name, file))

  private def uploadFile(service: AmazonS3, bucketName: String, key: String, file: File) =
    Sync[F].delay(Try(service.putObject(bucketName, key, file.toJava))).flatMap {
      case Success(result)    => (key, result).pure[F]
      case Failure(exception) => CannotUploadFile(exception).raiseError[F, (String, PutObjectResult)]
    }

  private def checkBucket(service: AmazonS3, bucketName: String) =
    if (service.doesBucketExistV2(bucketName)) {
      true.pure[F]
    } else {
      CannotGetBucket(s"Bucket not exists : $bucketName").raiseError[F, Boolean]
    }

  private def getService(accessKey: String, secretKey: String, region: String): F[AmazonS3] =
    createCredentials(accessKey, secretKey, region).flatMap {
      case Success(value)     => value.pure[F]
      case Failure(exception) => CannotGetService(exception).raiseError[F, AmazonS3]
    }

  private def createCredentials(accessKey: String, secretKey: String, region: String): F[Try[AmazonS3]] =
    Try(
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
        .withRegion(region)
        .build()
    ).pure[F]

  private def getBucketName: F[String] =
    getConfig("constellation.storage.aws.bucket-name").flatMap {
      case Success(bucketName) => bucketName.pure[F]
      case Failure(exception)  => CannotGetConfigProperty(exception).raiseError[F, String]
    }

  private def getAccessKey: F[String] =
    getConfig("constellation.storage.aws.aws-access-key").flatMap {
      case Success(accessKey) => accessKey.pure[F]
      case Failure(exception) => CannotGetConfigProperty(exception).raiseError[F, String]
    }

  private def getSecretKey: F[String] =
    getConfig("constellation.storage.aws.aws-secret-key").flatMap {
      case Success(secretKey) => secretKey.pure[F]
      case Failure(exception) => CannotGetConfigProperty(exception).raiseError[F, String]
    }

  private def getRegion: F[String] =
    getConfig("constellation.storage.aws.region").flatMap {
      case Success(region)    => region.pure[F]
      case Failure(exception) => CannotGetConfigProperty(exception).raiseError[F, String]
    }

  private def getConfig(configName: String): F[Try[String]] =
    Sync[F].delay(ConfigUtil.get(configName))
}
