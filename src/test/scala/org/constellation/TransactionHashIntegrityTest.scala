package org.constellation

import cats.effect.{ContextShift, IO}
import io.circe.generic.auto._
import io.circe.parser.parse
import org.constellation.keytool.KeyStoreUtils
import org.constellation.primitives.Schema.EdgeHashType.TransactionDataHash
import org.constellation.primitives.Schema.{ObservationEdge, TypedEdgeHash}
import org.constellation.primitives.Transaction
import org.constellation.serializer.KryoSerializer
import org.constellation.wallet.{Hashable, LastTransactionRef, KryoSerializer => WalletKryoSerializer, Transaction => WalletTransaction}
import org.scalatest.{FreeSpec, Matchers}

class TransactionHashIntegrityTest extends FreeSpec with Matchers {
  implicit val cc: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  val txPath = "src/test/resources/valid-tx.txt"

  "transaction read by wallet and node should have consistent hash" in {
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    val readTx2 = KeyStoreUtils
      .readFromFileStream[IO, Option[Transaction]](
        txPath,
        KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](parse(_).map(_.as[Transaction]).toOption.flatMap(_.toOption))
      )
      .value
      .unsafeRunSync()
      .right
      .get
      .get

    val walletHash1 = readTx.hash
    val nodeHash1 = readTx2.hash

    val serializedWalletTx = WalletKryoSerializer.serializeAnyRef(readTx)
    val serializedNodeTx = KryoSerializer.serializeAnyRef(readTx2)

    val deserializedWalletTx = WalletKryoSerializer.deserializeCast[WalletTransaction](serializedWalletTx)
    val deserializedNodeTx = KryoSerializer.deserializeCast[Transaction](serializedNodeTx)

    val walletHash2 = deserializedWalletTx.hash
    val nodeHash2 = deserializedNodeTx.hash

    walletHash1 shouldBe nodeHash1
    walletHash2 shouldBe nodeHash2

    walletHash1 shouldBe walletHash2
    nodeHash1 shouldBe nodeHash2
  }

  "Forged Transaction should be detected" in {
    val readTx = KeyStoreUtils
      .readFromFileStream[IO, Option[Transaction]](
        txPath,
        KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](parse(_).map(_.as[Transaction]).toOption.flatMap(_.toOption))
      )
      .value
      .unsafeRunSync()
      .right
      .get
      .get
    val dummyTypedEdgeHash = TypedEdgeHash("", TransactionDataHash)
    val dummyOE = ObservationEdge(Seq(), dummyTypedEdgeHash)

    readTx.edge.copy(observationEdge = dummyOE)
    assert(!readTx.isValid)
  }

  "transaction can be json encoded and decoded" in {
    val txPath = "src/test/resources/valid-tx.txt"
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    val jsonString = WalletTransaction.transactionToJsonString(readTx)

    val transaction = WalletTransaction.transactionFromJsonString(jsonString)

    Hashable.hash(readTx.edge.signedObservationEdge) shouldBe Hashable.hash(transaction.edge.signedObservationEdge)
  }
}
