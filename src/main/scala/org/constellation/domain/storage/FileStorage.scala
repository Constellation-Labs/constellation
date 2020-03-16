package org.constellation.domain.storage

import better.files.File
import cats.data.EitherT

trait FileStorage[F[_], A] {
  def exists(path: String): F[Boolean]

  def read(path: String): EitherT[F, Throwable, A]

  def readBytes(path: String): EitherT[F, Throwable, Array[Byte]]

  def getFile(path: String): EitherT[F, Throwable, File]

  def getFiles(): EitherT[F, Throwable, List[File]]

  def getFiles(paths: List[String]): EitherT[F, Throwable, List[File]]

  def write(path: String, bytes: Array[Byte]): EitherT[F, Throwable, Unit]

  def write(path: String, a: A): EitherT[F, Throwable, Unit]

  def write(file: File): EitherT[F, Throwable, Unit]

  def write(path: String, file: File): EitherT[F, Throwable, Unit]

  def delete(path: String): EitherT[F, Throwable, Unit]

  def list(): EitherT[F, Throwable, List[String]]
}
