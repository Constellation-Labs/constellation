package org.constellation.domain.checkpointBlock

import java.security.KeyPair

import cats.effect.{ContextShift, IO}
import org.constellation.Fixtures
import cats.implicits._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.transaction.TransactionChainService
import org.constellation.keytool.KeyUtils
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.edge.{Edge, EdgeHashType, ObservationEdge, TypedEdgeHash}
import org.constellation.schema.transaction
import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionEdgeData}
import org.constellation.serialization.KryoSerializer
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class CheckpointBlockDoubleSpendCheckerTest extends AnyFunSuite with BeforeAndAfter with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val kp: KeyPair = KeyUtils.makeKeyPair()

  private var blacklistedAddresses: BlacklistedAddresses[IO] = _
  private var transactionChainService: TransactionChainService[IO] = _

  before {
    KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()
    blacklistedAddresses = BlacklistedAddresses[IO]
    transactionChainService = TransactionChainService[IO]
  }

  test("it should return transaction when transaction's ordinal number is less than or equal the last accepted one") {
    val firstTransaction = createTransaction("src", "firstTransaction", 1)
    val secondTransaction = createTransaction("src", firstTransaction.hash, firstTransaction.ordinal)
    transactionChainService.acceptTransaction(firstTransaction).unsafeRunSync()
    transactionChainService.acceptTransaction(secondTransaction).unsafeRunSync()
    val transactionToValidate = createTransaction("src", firstTransaction.hash, firstTransaction.ordinal)
    val cb = CheckpointBlock
      .createCheckpointBlock(Seq(transactionToValidate), Seq.empty, Seq.empty, Seq.empty, Seq.empty)

    val result = CheckpointBlockDoubleSpendChecker
      .check(cb)(transactionChainService)
      .unsafeRunSync()

    result.size shouldBe 1
  }

  test("it should return empty list when transaction's ordinal number is higher than the last accepted one") {
    val firstTransaction = createTransaction("src", "firstTransaction", 1)
    transactionChainService.acceptTransaction(firstTransaction).unsafeRunSync()
    val transactionToValidate = createTransaction("src", firstTransaction.hash, firstTransaction.ordinal)
    val cb = CheckpointBlock
      .createCheckpointBlock(Seq(transactionToValidate), Seq.empty, Seq.empty, Seq.empty, Seq.empty)

    val result: List[Transaction] = CheckpointBlockDoubleSpendChecker
      .check(cb)(transactionChainService)
      .unsafeRunSync()

    result.size shouldBe 0
  }

  test("it should return transactions when transaction's ordinal numbers are equal") {
    val firstTx = createTransaction("src", "lastHash", 1)
    val secondTx = createTransaction("src", "lastHash", 1)
    val thirdTx = createTransaction("src", "lastHash", 2)
    val fourthTx = createTransaction("anotherSrc", "lastHash", 2)
    val cb = CheckpointBlock
      .createCheckpointBlock(Seq(firstTx, secondTx, thirdTx, fourthTx), Seq.empty, Seq.empty, Seq.empty, Seq.empty)

    val result: List[Transaction] = CheckpointBlockDoubleSpendChecker
      .checkTxsRefsIn(cb)

    result.size shouldBe 2
  }

  private def createTransaction(src: String, lastHash: String, lastOrdinal: Long): Transaction = {
    import constellation.signedObservationEdge

    val data = TransactionEdgeData(1L, LastTransactionRef(lastHash, lastOrdinal))
    val oe = ObservationEdge(
      Seq(TypedEdgeHash(src, EdgeHashType.AddressHash), TypedEdgeHash("dst", EdgeHashType.AddressHash)),
      TypedEdgeHash(data.hash, EdgeHashType.TransactionDataHash)
    )
    val soe = signedObservationEdge(oe)(Fixtures.tempKey)

    transaction.Transaction(Edge(oe, soe, data), LastTransactionRef(lastHash, lastOrdinal))
  }
}
