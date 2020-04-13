package org.constellation.domain.transaction
import org.constellation.util.Signable

case class LastTransactionRef(
  prevHash: String,
  ordinal: Long
) extends Signable {
  override def getEncoding = {
    val hashLengthString = prevHash.length.toString
    val ordinalLengthString = prevHash.length.toString
    val args = Seq(
      hashLengthString,
      prevHash,
      ordinalLengthString,
      ordinal.toString
    )
    runLengthEncoding(args: _*)
  }
}

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)
}
