package org.constellation.domain.storage

import java.io.{File => JFile}

import better.files._
import cats.data.EitherT
import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.serializer.KryoSerializer
import org.slf4j.Logger

abstract class LocalFileStorage[F[_], A](baseDir: String)(implicit F: Concurrent[F])
    extends FileStorage[F, A]
    with DiskSpace[F] {

  private lazy val logger = Slf4jLogger.getLogger[F]

  private lazy val dir: F[File] = F.delay {
    File(baseDir)
  }
  private lazy val jDir: F[JFile] = dir.flatMap(
    a =>
      F.delay {
        a.toJava
      }
  )

  def createDirectoryIfNotExists(): EitherT[F, Throwable, Unit] =
    dir.flatMap { a =>
      logger.debug(s"Creating directory: ${a.pathAsString}") >> F.delay {
        a.createDirectoryIfNotExists()
      }
    }.void.attemptT

  def exists(fileName: String): F[Boolean] =
    dir.flatMap { a =>
      F.delay {
        (a / fileName).exists
      }
    }

  def read(filename: String): EitherT[F, Throwable, A] =
    readBytes(filename).flatMap { a =>
      F.delay {
        KryoSerializer.deserializeCast[A](a)
      }.attemptT
    }

  def getFile(fileName: String): EitherT[F, Throwable, File] =
    dir.flatMap { a =>
      F.delay {
        a / fileName
      }
    }.attemptT

  def readBytes(fileName: String): EitherT[F, Throwable, Array[Byte]] =
    dir
      .map(_ / fileName)
      .flatMap { a =>
        F.delay {
          a.loadBytes
        }
      }
      .attemptT

  def getFiles(): EitherT[F, Throwable, List[File]] =
    dir.flatMap { a =>
      F.delay {
        a.children.toList
      }
    }.attemptT

  def getFiles(fileNames: List[String]): EitherT[F, Throwable, List[File]] =
    dir.flatMap { a =>
      F.delay {
        a.collectChildren(f => fileNames.contains(f.name)).toList
      }
    }.attemptT

  def write(fileName: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / fileName)
      .flatMap { a =>
        F.delay {
          a.writeByteArray(bytes)
        }
      }
      .void
      .attemptT

  def write(fileName: String, a: A): EitherT[F, Throwable, Unit] =
    F.delay {
      KryoSerializer.serialize[A](a)
    }.attemptT.flatMap {
      write(fileName, _)
    }

  def write(file: File): EitherT[F, Throwable, Unit] =
    for {
      bytes <- F.delay {
        file.loadBytes
      }.attemptT
      _ <- write(file.name, bytes)
    } yield ()

  override def write(path: String, file: File): EitherT[F, Throwable, Unit] =
    for {
      bytes <- F.delay {
        file.loadBytes
      }.attemptT
      _ <- write(path, bytes)
    } yield ()

  def delete(fileName: String): EitherT[F, Throwable, Unit] =
    dir
      .map(_ / fileName)
      .flatMap { a =>
        F.delay {
          a.delete()
        }
      }
      .void
      .attemptT

  def list(): EitherT[F, Throwable, List[String]] =
    getFiles().map(_.map(_.name))

  def getUsableSpace: F[Long] = jDir.flatMap { a =>
    F.delay {
      a.getUsableSpace
    }
  }

  def getOccupiedSpace: F[Long] = dir.flatMap { a =>
    F.delay {
      a.size
    }
  }
}
