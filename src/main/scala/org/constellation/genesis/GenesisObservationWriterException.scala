package org.constellation.genesis

sealed trait GenesisObservationWriterException extends Throwable {
  val exceptionMessage: String
}

case class CannotWriteGenesisObservationToDisk(message: String) extends GenesisObservationWriterException {
  override val exceptionMessage: String = message
}

object CannotWriteGenesisObservationToDisk {
  def apply(exception: Throwable) = new CannotWriteGenesisObservationToDisk(exception.getMessage)
}

case class CannotSendGenesisObservationToCloud(errorMessage: String) extends GenesisObservationWriterException {
  override val exceptionMessage: String = errorMessage
}
