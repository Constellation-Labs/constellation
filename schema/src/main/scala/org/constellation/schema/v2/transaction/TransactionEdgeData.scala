package org.constellation.schema.v2.transaction

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.v2.signature.Signable

import scala.util.Random

/**
  * Holder for ledger update information about a transaction
  *
  * @param amount : Quantity to be transferred
  * @param salt : Ensure hash uniqueness
  */
case class TransactionEdgeData(
  amount: Long,
  lastTxRef: LastTransactionRef,
  fee: Option[Long] = None,
  salt: Long = Random.nextLong()
) extends Signable {
  override def getEncoding = {
    val encodedAmount = runLengthEncoding(Seq(amount.toHexString): _*)
    val encodedFeeSalt = runLengthEncoding(Seq(fee.getOrElse(0L).toString, salt.toHexString): _*)
    encodedAmount + lastTxRef.getEncoding + encodedFeeSalt
  }
}

object TransactionEdgeData {
  implicit val transactionEdgeDataEncoder: Encoder[TransactionEdgeData] = deriveEncoder
  implicit val transactionEdgeDataDecoder: Decoder[TransactionEdgeData] = deriveDecoder
}
