package org.constellation.domain.storage

import better.files._
import java.io.{File => JFile}

import cats.data.EitherT
import cats.implicits._
import cats.effect.Concurrent
import org.constellation.domain.storage.StorageItemKind.StorageItemKind
import org.constellation.serializer.KryoSerializer

/**
  * Stores items by grouping items in directories by height
  *
  * Example:
  *  baseDir/
  *    - 2/
  *      - hashA_kindA
  *      - hashA_kindB
  *    - 4/
  *      - hashB_kindA
  *      - hashB_kindB
  *    - 6/
  *      - hashC_kindA
  *      - hashC_kindB
  *    - ...
  */
abstract class HeightBasedFileStorage[F[_], A](baseDir: String, kind: StorageItemKind)(implicit F: Concurrent[F]) {
  private lazy val suffix: String = s"-${StorageItemKind.toSuffix(kind)}"
  private lazy val dir: F[File] = F.delay { File(baseDir) }
  private lazy val jDir: F[JFile] = dir.flatMap(a => F.delay { a.toJava })

  def createBaseDirectoryIfNotExists(): EitherT[F, Throwable, Unit] =
    dir.flatMap { a =>
      F.delay { a.createDirectoryIfNotExists() }
    }.void.attemptT

  def createDirectoryIfNotExists(height: Long): EitherT[F, Throwable, File] =
    dir.flatMap { a =>
      F.delay { a.createChild(height.toString, asDirectory = true) }
    }.attemptT

  def exists(height: Long): F[Boolean] =
    dir.flatMap { a =>
      F.delay {
        a.glob(s"$height/${addSuffix("*")}").toList.nonEmpty
      }
    }

  def read(height: Long): EitherT[F, Throwable, A] =
    readBytes(height).flatMap { a =>
      F.delay {
        KryoSerializer.deserializeCast[A](a)
      }.attemptT
    }

  def getFile(height: Long): EitherT[F, Throwable, File] =
    dir.flatMap { a =>
      F.delay {
        a.glob(s"$height/${addSuffix("*")}").toList.head
      }
    }.attemptT

  def getDirectories(): EitherT[F, Throwable, List[File]] =
    dir.flatMap { a =>
      F.delay {
        a.collectChildren(_.isDirectory).toList
      }
    }.attemptT

  def readBytes(height: Long): EitherT[F, Throwable, Array[Byte]] =
    getFile(height).flatMap { a =>
      F.delay {
        a.loadBytes
      }.attemptT
    }

  def write(height: Long, hash: String, a: A): EitherT[F, Throwable, Unit] =
    F.delay {
      KryoSerializer.serialize(a)
    }.attemptT.flatMap(write(height, hash, _))

  def write(height: Long, hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    createDirectoryIfNotExists(height)
      .map(_ / addSuffix(hash))
      .flatMap { a =>
        F.delay { a.writeByteArray(bytes) }.attemptT
      }
      .void

  def delete(height: Long): EitherT[F, Throwable, Unit] =
    getFile(height).flatMap { a =>
      F.delay { a.delete() }.attemptT
    }.void

  def listDirectories(): EitherT[F, Throwable, List[Long]] =
    getDirectories().map(_.map(_.name.toLong))

  def getUsableSpace: F[Long] = jDir.flatMap { a =>
    F.delay { a.getUsableSpace }
  }

  def getOccupiedSpace: F[Long] = dir.flatMap { a =>
    F.delay { a.size }
  }

  private def addSuffix(fileName: String): String = s"$fileName$suffix"

  private def removeSuffix(suffixedFileName: String): String = suffixedFileName.stripSuffix(suffix)
}
