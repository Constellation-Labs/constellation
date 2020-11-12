package org.constellation.domain.checkpointBlock

import java.security.KeyPair

import cats.effect.{ContextShift, IO}
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.keytool.KeyUtils
import org.constellation.schema.v2.transaction.{LastTransactionRef, Transaction, TransactionEdgeData}
import org.constellation.schema.v2.checkpoint.CheckpointBlock
import org.constellation.schema.v2.edge.{Edge, EdgeHashType, ObservationEdge, TypedEdgeHash}
import org.constellation.schema.v2.transaction
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class CheckpointBlockBlacklistedAddressCheckerTest extends AnyFunSuite with BeforeAndAfter with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val kp: KeyPair = KeyUtils.makeKeyPair()

  private var blacklistedAddresses: BlacklistedAddresses[IO] = _

  before {
    blacklistedAddresses = BlacklistedAddresses[IO]
  }

  test("it should return invalid transaction when transaction has been made from blacklisted address") {
    val tx = createTransaction("blacklistSrcAddress")
    blacklistedAddresses.add(tx.src.address.toString).unsafeRunSync()
    val cb = CheckpointBlock
      .createCheckpointBlock(Seq(tx), Seq.empty, Seq.empty, Seq.empty, Seq.empty)

    val result = CheckpointBlockBlacklistedAddressChecker
      .check(cb)(blacklistedAddresses)
      .unsafeRunSync()

    result.size shouldBe 1
  }

  test("it should return empty list when transaction has been made from non blacklisted address") {
    val tx = createTransaction("correctSrcAddress")
    val cb = CheckpointBlock
      .createCheckpointBlock(Seq(tx), Seq.empty, Seq.empty, Seq.empty, Seq.empty)

    val result = CheckpointBlockBlacklistedAddressChecker
      .check(cb)(blacklistedAddresses)
      .unsafeRunSync()

    result.size shouldBe 0
  }

  test(
    "it should return invalid transaction if at least one of validated transactions has been made from blacklisted address"
  ) {
    val correctTx = createTransaction("correctSrcAddress")
    val blacklistTx = createTransaction("blacklistSrcAddress")
    blacklistedAddresses.add(blacklistTx.src.address.toString).unsafeRunSync()
    val cb = CheckpointBlock
      .createCheckpointBlock(Seq(correctTx, blacklistTx), Seq.empty, Seq.empty, Seq.empty, Seq.empty)

    val result = CheckpointBlockBlacklistedAddressChecker
      .check(cb)(blacklistedAddresses)
      .unsafeRunSync()

    result.size shouldBe 1
    result.head.src.address shouldBe blacklistTx.src.address
  }

  private def createTransaction(src: String, lastHash: String = "lastHash", lastOrdinal: Long = 0L): Transaction = {
    import constellation.signedObservationEdge
    val lastTxRef = LastTransactionRef(lastHash, lastOrdinal)
    val data = TransactionEdgeData(1L, lastTxRef, fee = None)
    val oe = ObservationEdge(
      Seq(TypedEdgeHash(src, EdgeHashType.AddressHash), TypedEdgeHash("dst", EdgeHashType.AddressHash)),
      TypedEdgeHash(data.hash, EdgeHashType.TransactionDataHash)
    )
    val soe = signedObservationEdge(oe)(Fixtures.tempKey)

    transaction.Transaction(Edge(oe, soe, data), LastTransactionRef("lastHash", 1))
  }

}
