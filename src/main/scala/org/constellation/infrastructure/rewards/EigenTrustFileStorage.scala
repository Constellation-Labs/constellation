package org.constellation.infrastructure.rewards

import better.files.File
import cats.data.EitherT
import cats.effect.Sync
import org.constellation.domain.rewards.{EigenTrustStorage, StoredEigenTrust}
import org.constellation.serializer.KryoSerializer
import better.files._
import cats.implicits._
import java.io.{File => JFile}

class EigenTrustFileStorage[F[_]](dirPath: String)(implicit F: Sync[F]) extends EigenTrustStorage[F] {
  private lazy val dir: F[File] = F.delay { File(dirPath) }

  private lazy val jDir: F[JFile] = dir.flatMap(a => F.delay { a.toJava })

  override def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit] =
    dir.flatMap { a =>
      F.delay { a.createDirectoryIfNotExists() }
    }.void.attemptT

  override def exists(hash: String): F[Boolean] =
    dir.flatMap(a => {
      F.delay { (a / hash).exists }
    })

  override def readEigenTrust(hash: String): EitherT[F, Throwable, StoredEigenTrust] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay {
          a.byteArray
        }
      }
      .flatMap { a =>
        F.delay {
          KryoSerializer.deserializeCast[StoredEigenTrust](a)
        }
      }
      .attemptT

  override def writeEigenTrust(hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.writeByteArray(bytes) }
      }
      .void
      .attemptT

  override def removeEigenTrust(hash: String): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.delete() }
      }
      .void
      .attemptT

  override def getUsableSpace: F[Long] = jDir.flatMap { a =>
    F.delay { a.getUsableSpace }
  }

  override def getOccupiedSpace: F[Long] = dir.flatMap { a =>
    F.delay { a.size }
  }

  override def getEigenTrustHashes: F[List[String]] = getEigenTrustFiles.map(_.map(_.name))

  override def getEigenTrustFiles: F[List[File]] = dir.flatMap { a =>
    F.delay {
      a.children.toList
    }
  }

  override def getEigenTrustFiles(hashes: List[String]): F[List[File]] =
    getEigenTrustFiles.map {
      _.filter { file =>
        hashes.contains(file.name)
      }
    }

  override def getEigenTrustBytes(hash: String): EitherT[F, Throwable, Array[Byte]] =
    dir
      .map(_ / hash)
      .flatMap { a =>
        F.delay { a.loadBytes }
      }
      .attemptT
}

object EigenTrustFileStorage {
  def apply[F[_]: Sync](dirPath: String): EigenTrustFileStorage[F] = new EigenTrustFileStorage[F](dirPath)
}
