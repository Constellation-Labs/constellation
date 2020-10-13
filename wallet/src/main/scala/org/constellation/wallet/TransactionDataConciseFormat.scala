package org.constellation.wallet

import java.nio.charset.StandardCharsets.US_ASCII

import org.constellation.keytool.KeyUtils.bytes2hex
import org.constellation.schema.transaction.Transaction

object TransactionDataConciseFormat {
  sealed trait EncodableValue[A] { val value: A }
  final case class EncodableLong(value: Long) extends EncodableValue[Long]
  final case class EncodableASCII(value: String) extends EncodableValue[String]

  private def encode[A](value: EncodableValue[A]): String = value match {
    case EncodableLong(value) =>
      val toEncode = BigInt(value).toByteArray
      encodeLength(toEncode.length) + bytes2hex(toEncode)
    case EncodableASCII(value) =>
      val toEncode = US_ASCII.encode(value).array()
      encodeLength(toEncode.length) + bytes2hex(toEncode)
  }

  private def encodeLength(len: Int): String = bytes2hex(BigInt(len).toByteArray)

  private def runLengthEncoding(values: Seq[EncodableValue[_]]): String = values.foldLeft("") { (acc, value) =>
    acc + encode(value)
  }

  def generate(tx: Transaction): String =
    encodeLength(tx.edge.parents.length) +
      runLengthEncoding(
        tx.edge.parents.map(p => EncodableASCII(p.hashReference)) ++
          Seq(
            EncodableLong(tx.amount),
            EncodableASCII(tx.lastTxRef.prevHash),
            EncodableLong(tx.lastTxRef.ordinal),
            EncodableLong(tx.fee.getOrElse(0L)),
            EncodableLong(tx.edge.data.salt)
          )
      )
}
