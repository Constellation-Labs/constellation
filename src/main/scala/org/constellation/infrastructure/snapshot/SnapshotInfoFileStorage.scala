package org.constellation.infrastructure.snapshot

import cats.effect.Sync
import cats.implicits._
import org.constellation.domain.snapshot.{SnapshotInfo, SnapshotInfoStorage}
import better.files._
import java.io.{File => JFile}

import cats.data.EitherT
import org.constellation.serializer.KryoSerializer

class SnapshotInfoFileStorage[F[_]](dirPath: String)(implicit F: Sync[F]) extends SnapshotInfoStorage[F] {
  private lazy val dir: F[File] = F.delay { File(dirPath) }
  private lazy val jDir: F[JFile] = dir.flatMap(a => F.delay { a.toJava })

  def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit] =
    dir.flatMap { a =>
      F.delay { a.createDirectoryIfNotExists() }
    }.void.attemptT

  def exists(hash: String): F[Boolean] =
    dir.flatMap { a =>
      F.delay { (a / hash).exists }
    }

  def readSnapshotInfo(hash: String): EitherT[F, Throwable, SnapshotInfo] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay {
          a.byteArray
        }
      }
      .flatMap { a =>
        F.delay {
          KryoSerializer.deserializeCast[SnapshotInfo](a)
        }
      }
      .attemptT

  def writeSnapshotInfo(hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.writeByteArray(bytes) }
      }
      .void
      .attemptT

  def removeSnapshotInfo(hash: String): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.delete() }
      }
      .void
      .attemptT

  def getSnapshotInfoHashes: F[List[String]] = getSnapshotInfoFiles.map(_.map(_.name))

  def getSnapshotInfoFiles: F[List[File]] = dir.flatMap { a =>
    F.delay {
      a.children.toList
    }
  }

  def getSnapshotInfoFiles(hashes: List[String]): F[List[File]] = getSnapshotInfoFiles.map {
    _.filter { file =>
      hashes.contains(file.name)
    }
  }

  def getSnapshotInfoBytes(hash: String): EitherT[F, Throwable, Array[Byte]] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.loadBytes }
      }
      .attemptT
}

object SnapshotInfoFileStorage {
  def apply[F[_]: Sync](dirPath: String): SnapshotInfoFileStorage[F] = new SnapshotInfoFileStorage[F](dirPath)
}
