package org.constellation.schema.v2.transaction

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.signature.Signable

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
