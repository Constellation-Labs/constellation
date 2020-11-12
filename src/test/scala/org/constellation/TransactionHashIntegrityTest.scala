/**
  * CREATE SEPARATE INTEGRATION TEST FOR THAT
  */

/*
package org.constellation

import cats.effect.{ContextShift, IO}
import io.circe.generic.auto._
import io.circe.parser.parse
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.schema.schema.snapshot.SnapshotInfo
import org.constellation.domain.storage.StorageItemKind.GenesisObservation
import org.constellation.infrastructure.snapshot.{SnapshotInfoLocalStorage, SnapshotLocalStorage}
import org.constellation.keytool.KeyStoreUtils
import org.constellation.schema.schema.transaction.Transaction
import org.constellation.serializer.KryoSerializer
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap

class TransactionHashIntegrityTest extends AnyFreeSpec with Matchers {
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

    readTx2.isValid shouldBe true
    readTx.isValid shouldBe true

    walletHash1 shouldBe nodeHash1
    walletHash2 shouldBe nodeHash2

    walletHash1 shouldBe walletHash2
    nodeHash1 shouldBe nodeHash2
  }

  "transaction created by wallet should keep proper tx chain" in {
    val src = "DAGaaa"
    val dst = "DAGbbb"
    val kp = Fixtures.tempKey

    val firstTx = WalletTransaction.createTransaction(None, src, dst, 2L, kp, None)
    val secondTx = WalletTransaction.createTransaction(Some(firstTx), src, dst, 2L, kp, None)

    secondTx.lastTxRef.prevHash shouldBe firstTx.hash
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
    val dummyTypedEdgeHash = TypedEdgeHash("dummyTypedEdgeHash", TransactionDataHash)
    val dummyOE = ObservationEdge(Seq(dummyTypedEdgeHash, dummyTypedEdgeHash), dummyTypedEdgeHash)
    val forgedEdge = readTx.edge.copy(observationEdge = dummyOE)
    val forgedTx = readTx.copy(edge = forgedEdge)
    assert(!forgedTx.isValid)
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

**/
