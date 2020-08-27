package org.constellation.wallet

import java.io.{FileInputStream, FileOutputStream}
import java.security.KeyPair

import cats.effect.Sync
import com.google.common.hash.Hashing
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

  def hash: String = edge.observationEdge.hash

  private def historicalRunLengthEncoding(hashes: String*): String = hashes.fold("")((acc, hash) => s"$acc${hash.length}$hash")

  def historicalDataRunLengthEncoding = {
    val data = edge.data

    historicalRunLengthEncoding(
      Seq(
        data.amount.toHexString,
        data.lastTxRef.prevHash,
        data.lastTxRef.ordinal.toString,
        data.fee.getOrElse(0L).toString,
        data.salt.toHexString
      ): _*
    )
  }

  def historicalHash: String = {
    val parents = edge.observationEdge.parents
    val rle =
      parents.length +
        historicalRunLengthEncoding(parents.map(_.hashReference): _*) ++
        historicalDataRunLengthEncoding

    Hashing.sha256().hashBytes(KryoSerializer.serializeAnyRef(rle)).toString
  }


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

  def pickHashForLastTransactionRef(tx: Transaction): LastTransactionRef = {
    val ordinal = tx.lastTxRef.ordinal + 1
    val isOldRLE = tx.edge.observationEdge.data.hashReference == tx.historicalDataRunLengthEncoding
    val isNewRLE = tx.edge.observationEdge.data.hashReference == tx.edge.data.getEncoding

    if (isOldRLE && !isNewRLE)
      LastTransactionRef(tx.historicalHash, ordinal)
    else
      LastTransactionRef(tx.hash, ordinal)
  }

  def createTransaction(
    prevTx: Option[Transaction] = None,
    src: String,
    dst: String,
    amount: Long,
    keyPair: KeyPair,
    fee: Option[Double] = None
  ): Transaction = {
    val lastTxRef =
      prevTx
        .map(pickHashForLastTransactionRef)
        .getOrElse(LastTransactionRef.empty)

    val edge = TransactionEdge.createTransactionEdge(src, dst, lastTxRef, amount, keyPair, fee)
    Transaction(edge, lastTxRef, false, false)
  }

}
