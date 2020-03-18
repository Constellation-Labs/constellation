package org.constellation.domain.cloud

import java.io.ByteArrayInputStream

import better.files.File
import cats.data.EitherT
import cats.effect.Concurrent
import cats.implicits._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.util.IOUtils
import org.constellation.infrastructure.cloud.{CannotGetBucket, CannotGetService}
import org.constellation.serializer.KryoSerializer

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

abstract class S3Storage[F[_], A](
  accessKey: String,
  secretKey: String,
  region: String,
  bucket: String,
  dir: Option[String] = None
)(
  implicit F: Concurrent[F]
) extends CloudStorage[F, A] {

  def bucketExists(): F[Boolean] =
    getService().flatMap(checkBucket)

  override def exists(path: String): F[Boolean] =
    for {
      s <- getService()
      objectExists <- F.delay {
        s.doesObjectExist(bucket, getDirPath(path))
      }
    } yield objectExists

  override def read(path: String): EitherT[F, Throwable, A] =
    readBytes(path).flatMap { b =>
      F.delay {
        KryoSerializer.deserializeCast[A](b)
      }.attemptT
    }

  override def readBytes(path: String): EitherT[F, Throwable, Array[Byte]] =
    (for {
      s <- getService()
      s3o <- F.delay {
        s.getObject(bucket, getDirPath(path))
      }
      s3is = s3o.getObjectContent
      bytes = IOUtils.toByteArray(s3is)
      _ <- F.delay {
        s3is.close()
      }
    } yield bytes).attemptT

  override def getFile(path: String): EitherT[F, Throwable, File] =
    EitherT.fromEither(Left(new Throwable("S3 doesn't support File instances")))

  override def getFiles(): EitherT[F, Throwable, List[File]] =
    EitherT.fromEither(Left(new Throwable("S3 doesn't support File instances")))

  override def getFiles(paths: List[String]): EitherT[F, Throwable, List[File]] =
    EitherT.fromEither(Left(new Throwable("S3 doesn't support File instances")))

  override def write(path: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    for {
      s <- getService().attemptT
      is = new ByteArrayInputStream(bytes)
      metadata <- F.delay {
        val metadata = new ObjectMetadata()
        metadata.setContentLength(bytes.length)
        metadata
      }.attemptT
      _ <- F.delay {
        s.putObject(bucket, getDirPath(path), is, metadata)
      }.attemptT
    } yield ()

  override def write(path: String, a: A): EitherT[F, Throwable, Unit] =
    F.delay {
      KryoSerializer.serialize[A](a)
    }.attemptT.flatMap(write(path, _))

  override def write(file: File): EitherT[F, Throwable, Unit] =
    for {
      s <- getService().attemptT
      _ <- F.delay {
        s.putObject(bucket, getDirPath(file.name), file.toJava)
      }.attemptT
    } yield ()

  override def write(path: String, file: File): EitherT[F, Throwable, Unit] =
    F.delay {
      file.loadBytes
    }.attemptT
      .flatMap(write(path, _))

  override def delete(path: String): EitherT[F, Throwable, Unit] =
    for {
      s <- getService().attemptT
      _ <- F.delay {
        s.deleteObject(bucket, getDirPath(path))
      }.attemptT
    } yield ()

  override def list(): EitherT[F, Throwable, List[String]] =
    for {
      s <- getService().attemptT
      summaries <- F.delay {
        s.listObjectsV2(bucket, dir.getOrElse("")).getObjectSummaries
      }.attemptT
      fileNames = summaries.asScala.toList
        .map(_.getKey)
        .map(_.stripPrefix(dir.map(d => s"${d}/").getOrElse("")))
        .map(_.split("/").head)
    } yield fileNames

  private def checkBucket(service: AmazonS3) =
    if (service.doesBucketExistV2(bucket)) {
      true.pure[F]
    } else {
      CannotGetBucket(s"Bucket not exists : $bucket").raiseError[F, Boolean]
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

  private def getDirPath(path: String): String =
    dir.map(d => s"$d/$path").getOrElse(path)
}
