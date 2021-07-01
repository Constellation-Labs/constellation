package org.constellation.schema.transaction

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.schema.signature.Signable
import org.constellation.schema.signature.Signable.runLengthEncoding

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
  implicit val lastTransactionRefDecoder: Decoder[LastTransactionRef] =
    deriveDecoder[LastTransactionRef].map(ltr => if (ltr == empty) empty else ltr)
  // Necessary for stable kryo serialization-deserialization of objects initially deserialized with circe. In kryo
  // references to different empty string instances are not preserved during deserialization and they're replaced with
  // a single reference to an empty string from Java string pool.
}
