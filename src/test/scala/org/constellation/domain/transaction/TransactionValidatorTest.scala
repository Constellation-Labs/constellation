package org.constellation.domain.transaction

import cats.data.Validated
import cats.effect.IO
import org.constellation.schema.v2.edge.EdgeHashType.{AddressHash, TransactionDataHash}
import org.constellation.schema.v2.signature.SignatureBatch
import org.constellation.schema.v2.transaction.{LastTransactionRef, Transaction, TransactionEdgeData}
import org.constellation.schema.v2.edge.{Edge, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.schema.v2.transaction
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class TransactionValidatorTest
    extends AnyFreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfter {

  import TransactionValidatorTest._

  var transactionChainService: TransactionChainService[IO] = _
  var transactionService: TransactionService[IO] = _
  var transactionValidator: TransactionValidator[IO] = _

  before {
    transactionChainService = mock[TransactionChainService[IO]]
    transactionService = mock[TransactionService[IO]]
    transactionService.transactionChainService shouldReturn transactionChainService
    transactionValidator = new TransactionValidator[IO](transactionService)
  }

  "validateLastTransactionRef" - {
    val src = "sender"
    val dst = "receiver"
    "should fail validation" - {
      "when transaction has different last transaction references in transaction and edge" in {
        transactionChainService.getLastAcceptedTransactionRef(src) shouldReturnF LastTransactionRef.empty
        val tx = createTransaction(src, dst, LastTransactionRef("def", 2L), LastTransactionRef("abc", 1L))
        val result = transactionValidator.validateLastTransactionRef(tx).unsafeRunSync

        result shouldBe Validated.invalidNel(InconsistentLastTxRef(tx))
      }

      "when transaction's last transaction reference has ordinal greater than 0 but an empty hash" in {
        transactionChainService.getLastAcceptedTransactionRef(src) shouldReturnF LastTransactionRef.empty
        val txLastTxRef = LastTransactionRef("", 2L)
        val tx = createTransaction(src, dst, txLastTxRef, txLastTxRef)
        val result = transactionValidator.validateLastTransactionRef(tx).unsafeRunSync

        result shouldBe Validated.invalidNel(NonZeroOrdinalButEmptyHash(tx))
      }

      "when transaction's last transaction reference has lower ordinal then stored last transaction ref" in {
        val txLastTxRef = LastTransactionRef("abc", 2L)
        val storedLastTxRef = LastTransactionRef("def", 3L)
        transactionChainService.getLastAcceptedTransactionRef(src) shouldReturnF storedLastTxRef
        val tx = createTransaction(src, dst, txLastTxRef, txLastTxRef)
        val result = transactionValidator.validateLastTransactionRef(tx).unsafeRunSync

        result shouldBe Validated.invalidNel(LastTxRefOrdinalLowerThenStoredLastTxRef(tx))
      }

      "when transaction's last transaction reference has the same ordinal but different hash then stored last transaction ref" in {
        val txLastTxRef = LastTransactionRef("abc", 2L)
        val storedLastTxRef = LastTransactionRef("def", 2L)
        transactionChainService.getLastAcceptedTransactionRef(src) shouldReturnF storedLastTxRef
        val tx = createTransaction(src, dst, txLastTxRef, txLastTxRef)
        val result = transactionValidator.validateLastTransactionRef(tx).unsafeRunSync

        result shouldBe Validated.invalidNel(SameOrdinalButDifferentHashForLastTxRef(tx))
      }
    }

    "should successfully pass validation" - {
      "when transaction's last transaction reference is the same as stored last transaction reference" in {
        val txLastTxRef = LastTransactionRef("abc", 2L)
        val storedLastTxRef = LastTransactionRef("abc", 2L)
        transactionChainService.getLastAcceptedTransactionRef(src) shouldReturnF storedLastTxRef
        val tx = createTransaction(src, dst, txLastTxRef, txLastTxRef)
        val result = transactionValidator.validateLastTransactionRef(tx).unsafeRunSync

        result shouldBe Validated.validNel(tx)
      }

      "when transaction's last transaction reference has higher ordinal then stored transaction reference ordinal" in {
        val txLastTxRef = LastTransactionRef("abc", 3L)
        val storedLastTxRef = LastTransactionRef("def", 2L)
        transactionChainService.getLastAcceptedTransactionRef(src) shouldReturnF storedLastTxRef
        val tx = createTransaction(src, dst, txLastTxRef, txLastTxRef)
        val result = transactionValidator.validateLastTransactionRef(tx).unsafeRunSync

        result shouldBe Validated.validNel(tx)
      }
    }
  }
}

object TransactionValidatorTest {

  def createTransaction(src: String, dst: String, lastTxRef: LastTransactionRef, edgeLastTxRef: LastTransactionRef) =
    transaction.Transaction(
      Edge(
        ObservationEdge(
          parents = Seq(TypedEdgeHash(src, AddressHash), TypedEdgeHash(dst, AddressHash)),
          data = TypedEdgeHash("", TransactionDataHash)
        ),
        SignedObservationEdge(
          signatureBatch = SignatureBatch("", Seq.empty)
        ),
        TransactionEdgeData(0L, edgeLastTxRef)
      ),
      lastTxRef
    )
}
