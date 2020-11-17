package org.constellation.domain.transaction

import cats.data.Validated
import cats.effect.IO
import cats.syntax.all._
import org.constellation.Fixtures
import org.constellation.schema.address.Address
import org.constellation.schema.edge.EdgeHashType.{AddressHash, TransactionDataHash}
import org.constellation.schema.signature.SignatureBatch
import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionEdgeData}
import org.constellation.schema.edge.{Edge, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.schema.transaction
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

  "validateDestinationAddress" - {
    val validAddress = Fixtures.id.address

    "should fail validation" - {
      "when destination address is smaller than 40 characters" in {
        val tx =
          createTransaction("sender", validAddress.substring(0, 39), LastTransactionRef.empty, LastTransactionRef.empty)
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }

      "when destination address is larger than 40 characters" in {
        val tx = createTransaction("sender", validAddress + "A", LastTransactionRef.empty, LastTransactionRef.empty)
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }

      "when destination address contains non base58 character" in {
        val tx = createTransaction(
          "sender",
          validAddress.substring(0, 39) + "%",
          LastTransactionRef.empty,
          LastTransactionRef.empty
        )
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }

      "when destination address contains whitespace character" in {
        val tx = createTransaction(
          "sender",
          s"${validAddress.substring(0, 39)} ",
          LastTransactionRef.empty,
          LastTransactionRef.empty
        )
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }

      "when destination address does not contain DAG prefix" in {
        val tx = createTransaction(
          "sender",
          s"ABC${validAddress.substring(3)} ",
          LastTransactionRef.empty,
          LastTransactionRef.empty
        )
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }

      "when destination address is equal to source address" in {
        val tx = createTransaction(validAddress, validAddress, LastTransactionRef.empty, LastTransactionRef.empty)
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }

      "when the parity number does not match" in {
        val par = validAddress.substring(4).filter(Character.isDigit).map(_.toString.toInt).sum % 9
        val invalidPar = if (par > 5) 0 else 9
        val tx = createTransaction(
          "sender",
          s"${validAddress.substring(0, 3)}${invalidPar}${validAddress.substring(4)} ",
          LastTransactionRef.empty,
          LastTransactionRef.empty
        )
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe Validated.invalidNel(InvalidDestinationAddress(tx))
      }
    }

    "should pass validation" - {
      "when the destination address is correct" in {
        val tx = createTransaction("sender", validAddress, LastTransactionRef.empty, LastTransactionRef.empty)
        val result = TransactionValidator.validateDestinationAddress(tx)

        result shouldBe tx.validNel
      }
    }
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
