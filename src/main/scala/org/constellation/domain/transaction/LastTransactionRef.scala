package org.constellation.domain.transaction
import org.constellation.wallet.Signable

case class LastTransactionRef(
  prevHash: String,
  ordinal: Long
) extends Signable  {
  override def getRunLengthEncoding = {
    val hashLengthString = prevHash.length.toString
    val ordinalLengthString = prevHash.length.toString
    hashLengthString :: prevHash :: ordinalLengthString :: ordinal.toString :: Nil mkString ""
  }
}

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)
}
