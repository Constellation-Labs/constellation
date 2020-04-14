package org.constellation.wallet

import java.io.{FileInputStream, FileOutputStream}
import java.security.KeyPair

import cats.effect.Sync
import org.constellation.keytool.KeyStoreUtils

case class Transaction(
  edge: Edge[TransactionEdgeData],
  lastTxRef: LastTransactionRef,
  isDummy: Boolean,
  isTest: Boolean
) {
  def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

  // TODO: Add proper exception on empty option

  def amount: Long = edge.data.amount

  def fee: Option[Long] = edge.data.fee

  def baseHash: String = edge.signedObservationEdge.baseHash

  def hash: String =
    edge.observationEdge.hash //todo recalculate ObservationEdge Hash here from TX contents and use it to compare signaturesHash

  def signaturesHash: String = edge.signedObservationEdge.signatureBatch.hash

  def isValid = signatures.exists { hs ⇒
    hs.address == edge.parents.head.hashReference && hs.valid(signaturesHash) && hash == signaturesHash
  }
}

object Transaction {

  import io.circe.generic.auto._
  import io.circe.parser.parse
  import io.circe.syntax._

  def transactionParser[F[_]](fis: FileInputStream)(implicit F: Sync[F]): F[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[F, Transaction](parse(_).map(_.as[Transaction]).toOption.flatMap(_.toOption))(fis)

  def transactionWriter[F[_]](t: Transaction)(implicit F: Sync[F]): FileOutputStream => F[Unit] = {
    (fos: FileOutputStream) =>
      KeyStoreUtils.writeTypeToFileStream[F, Transaction](_.asJson.noSpaces)(t)(fos)
  }

  def transactionToJsonString(transaction: Transaction): String =
    transaction.asJson.noSpaces

  def transactionFromJsonString(data: String): Transaction =
    parse(data).map(_.as[Transaction]).toOption.flatMap(_.toOption) match {
      case Some(tx) => tx
      case None     => throw new Error("Cannot parse transaction data")
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
        .map(tx => LastTransactionRef(Hashable.hash(tx.edge.signedObservationEdge), tx.lastTxRef.ordinal + 1))
        .getOrElse(LastTransactionRef.empty)
    val edge = TransactionEdge.createTransactionEdge(src, dst, lastTxRef, amount, keyPair, fee)

    Transaction(edge, lastTxRef, false, false)
  }

}
