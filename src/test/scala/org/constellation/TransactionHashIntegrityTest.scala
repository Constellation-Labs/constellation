package org.constellation

import cats.effect.{ContextShift, IO}
import org.constellation.keytool.KeyStoreUtils
import org.constellation.primitives.Transaction
import org.constellation.serializer.KryoSerializer
import org.constellation.wallet.{KryoSerializer => WalletKryoSerializer}
import org.constellation.wallet.{Hashable, Transaction => WalletTransaction}
import org.scalatest.{FreeSpec, Matchers}
import io.circe.generic.auto._
import io.circe.parser.parse

class TransactionHashIntegrityTest extends FreeSpec with Matchers {
  implicit val cc: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  "transaction read by wallet and node should have consistent hash" in {
    val txPath = "src/test/resources/valid-tx.txt"
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

    val walletHash1 = Hashable.hash(readTx.edge.signedObservationEdge)
    val nodeHash1 = readTx2.hash

    val serializedWalletTx = WalletKryoSerializer.serializeAnyRef(readTx)
    val serializedNodeTx = KryoSerializer.serializeAnyRef(readTx2)

    val deserializedWalletTx = WalletKryoSerializer.deserializeCast[WalletTransaction](serializedWalletTx)
    val deserializedNodeTx = KryoSerializer.deserializeCast[Transaction](serializedNodeTx)

    val walletHash2 = Hashable.hash(deserializedWalletTx.edge.signedObservationEdge)
    val nodeHash2 = deserializedNodeTx.hash

    walletHash1 shouldBe nodeHash1
    walletHash2 shouldBe nodeHash2

    walletHash1 shouldBe walletHash2
    nodeHash1 shouldBe nodeHash2
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
