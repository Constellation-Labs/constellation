package org.constellation.infrastructure.snapshot

import java.nio.file.Paths

import cats.data.EitherT
import cats.implicits._
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.snapshot.{SnapshotStorage, SnapshotStorageError}
import better.files._
import java.io.{File => JFile}

import cats.effect.Sync
import org.constellation.serializer.KryoSerializer

class SnapshotFileStorage[F[_]](dirPath: String)(implicit F: Sync[F]) extends SnapshotStorage[F] {

  private lazy val dir: F[File] = F.pure(File(dirPath))

  private lazy val jDir: F[JFile] = dir.map(_.toJava)

  def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit] =
    dir.flatMap { a =>
      F.delay { a.createDirectoryIfNotExists() }
    }.void.attemptT

  def exists(hash: String): F[Boolean] = dir.map(_ / hash).flatMap { a =>
    F.delay { a.exists }
  }

  def readSnapshot(hash: String): EitherT[F, Throwable, StoredSnapshot] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay {
          a.byteArray
        }
      }
      .flatMap { a =>
        F.delay {
          KryoSerializer.deserializeCast[StoredSnapshot](a)
        }
      }
      .attemptT

  def writeSnapshot(hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.writeByteArray(bytes) }
      }
      .void
      .attemptT

  def removeSnapshot(hash: String): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.delete() }
      }
      .void
      .attemptT

  def getUsableSpace: F[Long] = jDir.flatMap { a =>
    F.delay { a.getUsableSpace }
  }

  def getOccupiedSpace: F[Long] = dir.flatMap { a =>
    F.delay { a.size }
  }

  def getSnapshotHashes: F[Iterator[String]] = dir.flatMap { a =>
    F.delay { a.children.map(_.name) }
  }

  def getSnapshotFiles: F[Iterator[File]] = dir.flatMap { a =>
    F.delay { a.children }
  }

  def getSnapshotFiles(hashes: List[String]): F[Iterator[File]] = getSnapshotFiles.map {
    _.filter { file =>
      hashes.contains(file.name)
    }
  }

  def getSnapshotBytes(hash: String): EitherT[F, Throwable, Array[Byte]] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.loadBytes }
      }
      .attemptT
}

object SnapshotFileStorage {
  def apply[F[_]: Sync](dirPath: String): SnapshotFileStorage[F] = new SnapshotFileStorage[F](dirPath)
}
