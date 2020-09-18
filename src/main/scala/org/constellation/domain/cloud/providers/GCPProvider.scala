package org.constellation.domain.cloud.providers

import java.io.FileInputStream

import better.files.File
import cats.data.EitherT
import cats.effect.Concurrent
import cats.syntax.all._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.StorageOptions
import org.constellation.domain.cloud.config.{GCP, GCPInherit, PermissionFile}
import org.constellation.schema.GenesisObservation
import org.constellation.serializer.KryoSerializer

class GCPProvider[F[_]](credentials: GoogleCredentials, bucketName: String)(implicit F: Concurrent[F])
    extends CloudServiceProvider[F] {
  val name = s"gcp/${bucketName}"
  val storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService

  def storeSnapshot(snapshot: File, height: Long, hash: String): EitherT[F, Throwable, Unit] =
    writeFile(s"snapshots/${height}-${hash}/${hash}-snapshot", snapshot)

  def storeSnapshotInfo(snapshotInfo: File, height: Long, hash: String): EitherT[F, Throwable, Unit] =
    writeFile(s"snapshots/${height}-${hash}/${hash}-snapshot_info", snapshotInfo)

  def storeGenesis(genesisObservation: GenesisObservation): EitherT[F, Throwable, Unit] =
    writeClass("genesis/genesis", genesisObservation)

  private def write(path: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    F.delay { storage.get(bucketName).create(path, bytes) }.void.attemptT

  private def writeClass[A](path: String, a: A): EitherT[F, Throwable, Unit] =
    F.delay {
      KryoSerializer.serialize[A](a)
    }.attemptT.flatMap(write(path, _))

  private def writeFile(path: String, file: File): EitherT[F, Throwable, Unit] =
    F.delay {
      file.loadBytes
    }.attemptT
      .flatMap(write(path, _))
}

object GCPProvider {

  def apply[F[_]: Concurrent](config: GCP) = {
    val credentials = config.auth match {
      case GCPInherit() => GoogleCredentials.getApplicationDefault()
      case PermissionFile(pathToPermissionFile) =>
        GoogleCredentials.fromStream(new FileInputStream(pathToPermissionFile))
    }

    new GCPProvider[F](credentials, config.bucketName)
  }
}
