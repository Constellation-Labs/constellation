package org.constellation.domain.checkpointBlock

import java.security.KeyPair

import cats.effect.{ContextShift, IO}
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.transaction.{LastTransactionRef, TransactionChainService}
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, TransactionEdgeData, TypedEdgeHash}
import org.constellation.primitives.{CheckpointBlock, Edge, Transaction}
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class CheckpointBlockDoubleSpendCheckerTest extends FunSuite with BeforeAndAfter with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  private implicit val kp: KeyPair = KeyUtils.makeKeyPair()

  private var blacklistedAddresses: BlacklistedAddresses[IO] = _
  private var transactionChainService: TransactionChainService[IO] = _

  before {
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

    val result = CheckpointBlockDoubleSpendChecker
      .check(cb)(transactionChainService)
      .unsafeRunSync()

    result.size shouldBe 0
  }

  private def createTransaction(src: String, lastHash: String, lastOrdinal: Long): Transaction = {
    import constellation.signedObservationEdge

    val data = TransactionEdgeData(1L)
    val oe = ObservationEdge(
      Seq(TypedEdgeHash(src, EdgeHashType.AddressHash), TypedEdgeHash("dst", EdgeHashType.AddressHash)),
      TypedEdgeHash(data.hash, EdgeHashType.TransactionDataHash)
    )
    val soe = signedObservationEdge(oe)(Fixtures.tempKey)

    Transaction(Edge(oe, soe, data), LastTransactionRef(lastHash, lastOrdinal))
  }
}
