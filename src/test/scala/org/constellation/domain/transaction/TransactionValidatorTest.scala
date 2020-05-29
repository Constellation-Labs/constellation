package org.constellation.domain.transaction

import cats.data.Validated
import cats.effect.IO
import org.constellation.primitives.Schema.EdgeHashType.{AddressHash, TransactionDataHash}
import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, SignedObservationEdge, TransactionEdgeData, TypedEdgeHash}
import org.constellation.primitives.{Edge, Transaction}
import org.constellation.util.SignatureBatch
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class TransactionValidatorTest
  extends FreeSpec
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

  "address format validation" - {
    val validAddress = "DAG48nmxgpKhZzEzyc86y9oHotxxG57G8sBBwj34"
    val lastTxRef = LastTransactionRef.empty

    "validateSourceAddressFormat" - {
      "should successfully pass validation for source address encoded with ASCII characters only" in {
        val tx = createTransaction(validAddress, "", lastTxRef, lastTxRef)
        val result = TransactionValidator.validateSourceAddressFormat(tx)

        result shouldBe Validated.validNel(tx)
      }

      "should fail validation for source address containing non-ASCII characters" in {
        val tx = createTransaction(validAddress + "ζ", "", lastTxRef, lastTxRef)
        val result = TransactionValidator.validateSourceAddressFormat(tx)

        result shouldBe Validated.invalidNel(InvalidSourceAddressFormat(tx))
      }
    }

    "validateDestinationAddressFormat" - {
      "should successfully pass validation for destination address encoded with ASCII characters only" in {
        val tx = createTransaction("", validAddress, lastTxRef, lastTxRef)
        val result = TransactionValidator.validateDestinationAddressFormat(tx)

        result shouldBe Validated.validNel(tx)
      }

      "should fail validation for destination address containing non-ASCII characters" in {
        val tx = createTransaction("", validAddress + "ζ", lastTxRef, lastTxRef)
        val result = TransactionValidator.validateDestinationAddressFormat(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddressFormat(tx))
      }
    }
  }

  "validateLastTxRefFormat" - {
    val validPrevHash = "d5149e2339ced3b285062dc403ba0c89642792a462476dc35f63e0328b3cac52"

    "should successfully pass validation for previous hash encoded with ASCII characters only" in {
      val lastTxRef = LastTransactionRef(validPrevHash, 1)
      val tx = createTransaction("", "", lastTxRef, lastTxRef)
      val result = TransactionValidator.validateLastTxRefFormat(tx)

      result shouldBe Validated.validNel(tx)
    }

    "should fail validation for previous hash containing non-ASCII characters" in {
      val lastTxRef = LastTransactionRef(validPrevHash + "ζ", 1)
      val tx = createTransaction("", "", lastTxRef, lastTxRef)
      val result = TransactionValidator.validateLastTxRefFormat(tx)

      result shouldBe Validated.invalidNel(IncorrectLastTxRefFormat(tx))
    }
  }
}

object TransactionValidatorTest {
  def createTransaction(src: String, dst: String, lastTxRef: LastTransactionRef, edgeLastTxRef: LastTransactionRef) =
    Transaction(
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
