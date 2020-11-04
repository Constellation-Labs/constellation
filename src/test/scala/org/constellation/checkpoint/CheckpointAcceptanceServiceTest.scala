package org.constellation.checkpoint

import cats.effect.{ContextShift, IO}
import cats.syntax.applicative._
import org.constellation.domain.transaction.TransactionChainService
import org.constellation.schema.address.Address
import org.constellation.schema.transaction.{LastTransactionRef, Transaction}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class CheckpointAcceptanceServiceTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfter
    with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val txChainService: TransactionChainService[IO] = mock[TransactionChainService[IO]]

  /** Represents transaction's `prevHash` and `hash` respectively */
  type Link = (String, String)

  before {
    txChainService.getLastAcceptedTransactionRef("src") shouldReturn LastTransactionRef("a", 0).pure[IO]
    txChainService.getLastAcceptedTransactionRef("SRC") shouldReturn LastTransactionRef("A", 0).pure[IO]
  }

  "areTransactionsAllowedForAcceptance" - {
    "should pass validation when transactions form a consistent chain" in {
      val txs = List(
        "src" -> List("a" -> "b", "b" -> "c", "c" -> "d"),
        "SRC" -> List("A" -> "B", "B" -> "C")
      ).flatMap(createTxs(1))

      val result = CheckpointAcceptanceService
        .areTransactionsAllowedForAcceptance[IO](txs)(txChainService)
        .unsafeRunSync

      result shouldBe true
    }

    "should fail validation when link b -> c is missing" in {
      val txs = List(
        "src" -> List("a" -> "b", "c" -> "d"),
        "SRC" -> List("A" -> "B", "B" -> "C")
      ).flatMap(createTxs(1))

      val result = CheckpointAcceptanceService
        .areTransactionsAllowedForAcceptance[IO](txs)(txChainService)
        .unsafeRunSync

      result shouldBe false
    }

    "should fail validation when last accepted transaction hash doesn't match the first transaction's prevHash" in {
      val txs = List(
        "src" -> List("b" -> "c", "c" -> "d"),
        "SRC" -> List("A" -> "B", "B" -> "C")
      ).flatMap(createTxs(1))

      val result = CheckpointAcceptanceService
        .areTransactionsAllowedForAcceptance[IO](txs)(txChainService)
        .unsafeRunSync

      result shouldBe false
    }

    "should fail validation when difference between ordinals is greater than 1" in {
      val txs1 = List("src" -> List("a" -> "b", "b" -> "c")).flatMap(createTxs(1))
      val txs2 = List("src" -> List("c" -> "d")).flatMap(createTxs(10))

      val result = CheckpointAcceptanceService
        .areTransactionsAllowedForAcceptance[IO](txs1 ++ txs2)(txChainService)
        .unsafeRunSync

      result shouldBe false
    }
  }

  def createTxs(ordinal: Long)(entry: (String, List[Link])): List[Transaction] =
    entry match {
      case (src, (prevHash, hash) :: tail) =>
        val tx = mock[Transaction]
        tx.src shouldReturn Address(src)
        tx.hash shouldReturn hash
        tx.ordinal shouldReturn ordinal
        tx.lastTxRef shouldReturn LastTransactionRef(prevHash, ordinal - 1)
        tx :: createTxs(ordinal + 1)((src, tail))
      case (_, Nil) => List.empty
    }
}
