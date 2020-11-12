package org.constellation.domain.cloud.providers

import java.io.ByteArrayInputStream

import better.files.File
import cats.data.EitherT
import cats.syntax.all._
import cats.effect.Concurrent
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.constellation.domain.cloud.config.{Credentials, S3, S3Compat, S3Inherit}
import org.constellation.schema.v2.GenesisObservation
import org.constellation.serializer.KryoSerializer

class S3Provider[F[_]](client: AmazonS3, bucketName: String)(implicit F: Concurrent[F])
    extends CloudServiceProvider[F] {

  val name = s"s3/${bucketName}"

  def storeSnapshot(snapshot: File, height: Long, hash: String): EitherT[F, Throwable, Unit] =
    writeFile(s"snapshots/${height}-${hash}/${hash}-snapshot", snapshot)

  def storeSnapshotInfo(snapshotInfo: File, height: Long, hash: String): EitherT[F, Throwable, Unit] =
    writeFile(s"snapshots/${height}-${hash}/${hash}-snapshot_info", snapshotInfo)

  def storeGenesis(genesisObservation: GenesisObservation): EitherT[F, Throwable, Unit] =
    writeClass("genesis/genesis", genesisObservation)

  private def write(
    path: String,
    bytes: Array[Byte]
  ): EitherT[F, Throwable, Unit] =
    for {
      is <- F.delay { new ByteArrayInputStream(bytes) }.attemptT
      metadata <- F.delay {
        val metadata = new ObjectMetadata()
        metadata.setContentLength(bytes.length)
        metadata
      }.attemptT
      _ <- F.delay {
        client.putObject(bucketName, path, is, metadata)
      }.attemptT
    } yield ()

  private def writeClass[A <: AnyRef](path: String, a: A): EitherT[F, Throwable, Unit] =
    F.delay {
      KryoSerializer.serializeAnyRef(a)
    }.attemptT.flatMap(write(path, _))

  def writeFile(path: String, file: File): EitherT[F, Throwable, Unit] =
    F.delay {
      file.loadBytes
    }.attemptT
      .flatMap(write(path, _))
}

object S3Provider {

  def apply[F[_]: Concurrent](config: S3): S3Provider[F] = {
    val client = config.auth match {
      case S3Inherit() =>
        AmazonS3ClientBuilder
          .standard()
          .withRegion(config.region)
          .withCredentials(
            new DefaultAWSCredentialsProviderChain()
          )
          .build()
      case Credentials(accessKey, secretKey) =>
        AmazonS3ClientBuilder
          .standard()
          .withRegion(config.region)
          .withCredentials(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
          )
          .build()
    }

    new S3Provider[F](client, config.bucketName)
  }

  def apply[F[_]: Concurrent](config: S3Compat): S3Provider[F] = {
    val client = AmazonS3ClientBuilder
      .standard()
      .withEndpointConfiguration(new EndpointConfiguration(config.endpoint, config.region))
      .withCredentials(
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.auth.accessKey, config.auth.secretKey))
      )
      .build()

    new S3Provider[F](client, config.bucketName)
  }
}
