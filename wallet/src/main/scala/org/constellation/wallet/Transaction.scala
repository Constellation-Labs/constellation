package org.constellation.wallet

import java.io.{FileInputStream, FileOutputStream}
import java.security.KeyPair

import cats.effect.{IO, Sync}
import com.google.common.hash.Hashing
import org.constellation.keytool.KeyStoreUtils
import org.json4s.ext.EnumNameSerializer
import org.json4s.{CustomKeySerializer, DefaultFormats, Extraction, Formats, JValue}
import org.json4s.native.{Serialization, parseJsonOpt}
import Implicits._
case class Transaction(
  edge: Edge[TransactionEdgeData],
  lastTxRef: LastTransactionRef,
  isDummy: Boolean,
  isTest: Boolean
)

object SerializerFormats {

  class IdSerializer
      extends CustomKeySerializer[Id](
        format =>
          ({
            case s: String =>
              Id(s)
          }, {
            case id: Id =>
              id.hex
          })
      )
}

trait Signable {

  def signInput: Array[Byte] = hash.getBytes()

  def hash: String = this.kryo.sha256

  def short: String = hash.slice(0, 5)

}

object Transaction {

  val formats: Formats = DefaultFormats + new SerializerFormats.IdSerializer + new EnumNameSerializer(EdgeHashType)

  implicit class SerExt(jsonSerializable: Any) {
    def compactRender(msg: JValue): String = Serialization.write(msg)(formats)

    def caseClassToJson(message: Any): String =
      compactRender(Extraction.decompose(message)(formats))
    def json: String = caseClassToJson(jsonSerializable)
  }

  implicit class ParseExt(input: String) {
    def parse4s(msg: String): JValue = parseJsonOpt(msg).get
    def jValue: JValue = parse4s(input)
    def x[T](implicit m: Manifest[T]): T = jValue.extract[T](formats, m)
  }

  def transactionParser[F[_]](fis: FileInputStream)(implicit F: Sync[F]): F[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[F, Transaction](ParseExt(_).x[Transaction])(fis)

  def transactionWriter[F[_]](t: Transaction)(implicit F: Sync[F]): FileOutputStream => F[Unit] = {
    (fos: FileOutputStream) =>
      KeyStoreUtils.writeTypeToFileStream[F, Transaction](SerExt(_).json)(t)(fos)
  }

  def createTransaction(
    prevTx: Option[Transaction] = None,
    src: String,
    dst: String,
    amount: Double,
    keyPair: KeyPair,
    fee: Option[Double] = None
  ): Transaction = {
    val lastTxRef =
      prevTx
        .map(tx => LastTransactionRef(tx.edge.signedObservationEdge.hash, tx.lastTxRef.ordinal + 1))
        .getOrElse(LastTransactionRef.empty)
    val edge = TransactionEdge.createTransactionEdge(src, dst, lastTxRef, amount, keyPair, fee)

    Transaction(edge, lastTxRef, false, false)
  }

}
