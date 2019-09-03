package org.constellation.storage.external

sealed trait CloudStorageException extends Throwable {
  val exceptionMessage: String
}

object CannotGetConfigProperty {
  def apply(exception: Throwable) = new CannotGetService(s"Cannot get config property : ${exception.getMessage}")
}

case class CannotGetConfigProperty(errorMessage: String) extends CloudStorageException {
  val exceptionMessage: String = errorMessage
}

object CannotGetService {
  def apply(exception: Throwable) = new CannotGetService(s"Cannot get service : ${exception.getMessage}")
}

case class CannotGetService(errorMessage: String) extends CloudStorageException {
  val exceptionMessage: String = errorMessage
}

object CannotGetBucket {
  def apply(exception: Throwable) = new CannotGetBucket(s"Cannot get bucket : ${exception.getMessage}")
}

case class CannotGetBucket(errorMessage: String) extends CloudStorageException {
  val exceptionMessage: String = errorMessage
}

object CannotUploadFile {
  def apply(exception: Throwable) = new CannotGetBucket(s"Cannot upload file : ${exception.getMessage}")
}

case class CannotUploadFile(errorMessage: String) extends CloudStorageException {
  val exceptionMessage: String = errorMessage
}
