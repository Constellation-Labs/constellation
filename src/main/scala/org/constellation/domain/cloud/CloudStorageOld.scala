package org.constellation.domain.cloud

import better.files.File
import cats.data.EitherT
import org.constellation.domain.storage.FileStorage

trait CloudStorageOld[F[_]] {
  def upload(files: Seq[File], dir: Option[String] = None): F[List[String]]
}

trait HeightHashFileStorage[F[_], A] extends FileStorage[F, A] {
  val fileSuffix: String

  def exists(height: Long, hash: String): F[Boolean] =
    exists(getHeightHashPath(height, hash))

  def read(height: Long, hash: String): EitherT[F, Throwable, A] =
    read(getHeightHashPath(height, hash))

  def readBytes(height: Long, hash: String): EitherT[F, Throwable, Array[Byte]] =
    readBytes(getHeightHashPath(height, hash))

  def getFile(height: Long, hash: String): EitherT[F, Throwable, File] =
    getFile(getHeightHashPath(height, hash))

  def write(height: Long, hash: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit] =
    write(getHeightHashPath(height, hash), bytes)

  def write(height: Long, hash: String, a: A): EitherT[F, Throwable, Unit] =
    write(getHeightHashPath(height, hash), a)

  def write(height: Long, hash: String, file: File): EitherT[F, Throwable, Unit] =
    write(getHeightHashPath(height, hash), file)

  def delete(height: Long, hash: String): EitherT[F, Throwable, Unit] =
    delete(getHeightHashPath(height, hash))

  private def getHeightHashDirectory(height: Long, hash: String): String =
    s"${height}-${hash}"

  private def getHeightHashFileName(height: Long, hash: String): String =
    s"${hash}-${fileSuffix}"

  private def getHeightHashPath(height: Long, hash: String): String =
    s"${getHeightHashDirectory(height, hash)}/${getHeightHashFileName(height, hash)}"
}

abstract class CloudStorage[F[_], A] extends FileStorage[F, A] {}
