package org.constellation.infrastructure.cloud

import better.files.File
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.{PutObjectResult, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.util.IOUtils
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.cloud.CloudStorage.StorageName
import org.constellation.domain.cloud.CloudStorage.StorageName.StorageName

import scala.util.{Failure, Success, Try}

class AWSStorage[F[_]: Concurrent](accessKey: String, secretKey: String, region: String, bucketName: String)
    extends CloudStorage[F] {

  private val logger = Slf4jLogger.getLogger[F]

  def upload(files: Seq[File], storageName: StorageName): F[List[String]] = {
    val upload = for {
      service <- getService()
      _ <- logger.debug("[CloudStorage] Service created successfully")

      _ <- checkBucket(service)
      _ <- logger.debug("[CloudStorage] Bucket got successfully")

      result <- saveToBucket(service, files, StorageName.toDirectory(storageName))
      _ <- logger.debug("[CloudStorage] Files saved successfully")
    } yield result.map(_._1)

    upload.handleErrorWith(
      err =>
        logger.error(
          s"[CloudStorage] Cannot upload files : AWS : ${err.getMessage} ${err.getStackTrace.map(_.toString).mkString(", ")}"
        ) >> Sync[F]
          .pure(
            List.empty[String]
          )
    )
  }

  def get(filename: String, storageName: StorageName): F[Array[Byte]] =
    for {
      service <- getService()
      _ <- checkBucket(service)
      bytes <- getFromBucket(service, filename, StorageName.toDirectory(storageName))
    } yield bytes

  private def saveToBucket(service: AmazonS3, files: Seq[File], dir: String) =
    files.toList.traverse(file => uploadFile(service, file.name, dir, file))

  private def getFromBucket(service: AmazonS3, filename: String, dir: String): F[Array[Byte]] =
    for {
      s3o <- Sync[F].delay { Try(service.getObject(bucketName, s"$dir/$filename")) }.flatMap {
        case Success(result)    => result.pure[F]
        case Failure(exception) => CannotGetFile(exception).raiseError[F, S3Object]
      }
      s3is = s3o.getObjectContent
      bytes = IOUtils.toByteArray(s3is)
      _ <- Sync[F].delay { s3is.close() }
    } yield bytes

  private def uploadFile(service: AmazonS3, key: String, dir: String, file: File) =
    Sync[F].delay(Try(service.putObject(bucketName, s"$dir/$key", file.toJava))).flatMap {
      case Success(result)    => (key, result).pure[F]
      case Failure(exception) => CannotUploadFile(exception).raiseError[F, (String, PutObjectResult)]
    }

  private def checkBucket(service: AmazonS3) =
    if (service.doesBucketExistV2(bucketName)) {
      true.pure[F]
    } else {
      CannotGetBucket(s"Bucket not exists : $bucketName").raiseError[F, Boolean]
    }

  private def getService(): F[AmazonS3] =
    createCredentials().flatMap {
      case Success(value)     => value.pure[F]
      case Failure(exception) => CannotGetService(exception).raiseError[F, AmazonS3]
    }

  private def createCredentials(): F[Try[AmazonS3]] =
    Try(
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
        .withRegion(region)
        .build()
    ).pure[F]
}
