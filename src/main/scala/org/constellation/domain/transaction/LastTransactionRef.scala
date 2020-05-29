package org.constellation.domain.transaction
import org.constellation.util.Signable
import org.constellation.wallet.EncodableValue._

case class LastTransactionRef(
  prevHash: String,
  ordinal: Long
) extends Signable {
  override def getEncoding: String = {
    val args = Seq(EncodableASCII(prevHash), EncodableLong(ordinal))
    runLengthEncoding(args)
  }
}

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)
}
