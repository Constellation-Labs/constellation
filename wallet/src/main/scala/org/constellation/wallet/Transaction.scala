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
)

object Transaction {

  import io.circe.generic.auto._
  import io.circe.syntax._
  import io.circe.parser.parse

  def transactionParser[F[_]](fis: FileInputStream)(implicit F: Sync[F]): F[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[F, Transaction](parse(_).map(_.as[Transaction]).toOption.flatMap(_.toOption))(fis)

  def transactionWriter[F[_]](t: Transaction)(implicit F: Sync[F]): FileOutputStream => F[Unit] = {
    (fos: FileOutputStream) =>
      KeyStoreUtils.writeTypeToFileStream[F, Transaction](_.asJson.noSpaces)(t)(fos)
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
