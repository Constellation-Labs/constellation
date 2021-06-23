package org.constellation.wallet

import java.io.{FileInputStream, FileOutputStream, OutputStream}
import java.security.KeyPair

import cats.effect.Sync
import org.constellation.keytool.KeyStoreUtils
import org.constellation.schema.edge.{Edge, EdgeHashType, ObservationEdge, TypedEdgeHash}
import org.constellation.schema.signature.SignHelp
import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionEdgeData}

object TransactionExt {

  import io.circe.generic.auto._
  import io.circe.parser.parse
  import io.circe.syntax._

  def createTransactionEdge(
    src: String,
    dst: String,
    lastTxRef: LastTransactionRef,
    amount: Long,
    keyPair: KeyPair,
    fee: Option[Double] = None
  ): Edge[TransactionEdgeData] = {
    val feeToUse = fee.map(_ * 1e8.toLong).map(_.toLong)
    val txData = TransactionEdgeData(amount, lastTxRef, feeToUse)
    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.getEncoding, EdgeHashType.TransactionDataHash)
    )
    val soe = SignHelp.signedObservationEdge(oe)(keyPair)
    Edge(oe, soe, txData)
  }

  def transactionParser[F[_]](fis: FileInputStream)(implicit F: Sync[F]): F[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[F, Transaction](parse(_).map(_.as[Transaction]).toOption.flatMap(_.toOption))(fis)

  def transactionWriter[F[_]](t: Transaction)(implicit F: Sync[F]): OutputStream => F[Unit] = { (fos: OutputStream) =>
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
    txRef: LastTransactionRef,
    forcePrevTx: Boolean,
    src: String,
    dst: String,
    amount: Long,
    keyPair: KeyPair,
    fee: Option[Double] = None
  ): Transaction = {
    val lastTxRef =
      prevTx
        .map(tx => LastTransactionRef(tx.hash, tx.ordinal))
        .getOrElse(LastTransactionRef.empty)

    if (lastTxRef != txRef && !forcePrevTx) {
      println(s"Previous transaction ref is different, taking one from the cluster!")
    }

    val correctReference = if (forcePrevTx) lastTxRef else txRef

    val edge = createTransactionEdge(src, dst, correctReference, amount, keyPair, fee)
    Transaction(edge, correctReference, false, false)
  }

}
