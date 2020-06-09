package org.constellation.domain.transaction

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.util.Signable

case class LastTransactionRef(
  prevHash: String,
  ordinal: Long
) extends Signable {
  override def getEncoding = {
    val args = Seq(prevHash, ordinal.toString)
    runLengthEncoding(args: _*)
  }
}

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)

  implicit val lastTransactionRefEncoder: Encoder[LastTransactionRef] = deriveEncoder
  implicit val lastTransactionRefDecoder: Decoder[LastTransactionRef] = deriveDecoder
}
