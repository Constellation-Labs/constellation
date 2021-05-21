package org.constellation.domain.checkpointBlock

import cats.data.Validated
import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.syntax._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointBlockValidator, InsufficientBalance}
import org.constellation.domain.transaction.{TransactionChainService, TransactionService, TransactionValidator}
import org.constellation.schema.address.{Address, AddressCacheData}
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointEdge, CheckpointEdgeData}
import org.constellation.schema.edge.{Edge, ObservationEdge, SignedObservationEdge}
import org.constellation.schema.signature.HashSignature
import org.constellation.schema.transaction.{LastTransactionRef, Transaction}
import org.constellation.storage.{AddressService, SnapshotService}
import org.constellation.{DAO, Fixtures, TestHelpers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class CheckpointBlockValidatorTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  var dao: DAO = _
  var snapService: SnapshotService[IO] = _
  var addressService: AddressService[IO] = _
  var transactionService: TransactionService[IO] = _
  var transactionChainService: TransactionChainService[IO] = _
  var checkpointParentService: CheckpointParentService[IO] = _
  var transactionValidator: TransactionValidator[IO] = _
  var cbValidator: CheckpointBlockValidator[IO] = _

  /** Represents transaction's `prevHash` and `hash` respectively */
  type Link = (String, String)

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

  before {
    dao = TestHelpers.prepareMockedDAO()
    snapService = mock[SnapshotService[IO]]
    addressService = mock[AddressService[IO]]
    transactionService = mock[TransactionService[IO]]
    checkpointParentService = mock[CheckpointParentService[IO]]
    transactionValidator = mock[TransactionValidator[IO]]
    transactionChainService = mock[TransactionChainService[IO]]
    cbValidator = new CheckpointBlockValidator[IO](
      addressService,
      snapService,
      checkpointParentService,
      transactionValidator,
      transactionChainService,
      dao
    )
    snapService.lastSnapshotHeight shouldReturn Ref.unsafe(1)
    transactionChainService.getLastAcceptedTransactionRef("src") shouldReturn LastTransactionRef("a", 0).pure[IO]
    transactionChainService.getLastAcceptedTransactionRef("SRC") shouldReturn LastTransactionRef("A", 0).pure[IO]
  }

  "validateLastTxRefChain" - {
    "should pass validation when transactions form a consistent chain" in {
      val txs = List(
        "src" -> List("a" -> "b", "b" -> "c", "c" -> "d"),
        "SRC" -> List("A" -> "B", "B" -> "C")
      ).flatMap(createTxs(1))

      val result = cbValidator.validateLastTxRefChain(txs).unsafeRunSync

      result.isValid shouldBe true
    }

    "should fail validation when link b -> c is missing" in {
      val txs = List(
        "src" -> List("a" -> "b", "c" -> "d"),
        "SRC" -> List("A" -> "B", "B" -> "C")
      ).flatMap(createTxs(1))

      val result = cbValidator.validateLastTxRefChain(txs).unsafeRunSync

      result.isValid shouldBe false
    }

    "should fail validation when last accepted transaction hash doesn't match the first transaction's prevHash" in {
      val txs = List(
        "src" -> List("b" -> "c", "c" -> "d"),
        "SRC" -> List("A" -> "B", "B" -> "C")
      ).flatMap(createTxs(1))

      val result = cbValidator.validateLastTxRefChain(txs).unsafeRunSync

      result.isValid shouldBe false
    }

    "should fail validation when difference between ordinals is greater than 1" in {
      val txs1 = List("src" -> List("a" -> "b", "b" -> "c")).flatMap(createTxs(1))
      val txs2 = List("src" -> List("c" -> "d")).flatMap(createTxs(10))

      val result = cbValidator.validateLastTxRefChain(txs1 ++ txs2).unsafeRunSync

      result.isValid shouldBe false
    }
  }

  "invalid checkpoint due to " - {

    "empty signatures " in {
      val checkpointBlock: CheckpointBlock = mock[CheckpointBlock]
      val mockHashSignature: HashSignature = mock[HashSignature]
      val mockCE: CheckpointEdge = mock[CheckpointEdge]
      val mockEdge: Edge[CheckpointEdgeData] = mock[Edge[CheckpointEdgeData]]
      val mocOE: ObservationEdge = mock[ObservationEdge]
      val mockSOE: SignedObservationEdge = mock[SignedObservationEdge]

      checkpointBlock.transactions shouldReturn Seq()
      mockHashSignature.valid(*) shouldReturn true
      checkpointBlock.signatures shouldReturn Seq()
      checkpointBlock.checkpoint shouldReturn mockCE
      mockCE.edge shouldReturn mockEdge
      mockEdge.observationEdge shouldReturn mocOE
      mocOE.hash shouldReturn "forgedOE"
      mocOE.toString shouldReturn "mockedOE"
      mockEdge.signedObservationEdge shouldReturn mockSOE
      mockSOE.hash shouldReturn "copiedSOE"
      mockSOE.toString shouldReturn "mockedSOE"

      val validationRes = cbValidator.simpleValidation(checkpointBlock)
      validationRes.unsafeRunSync.isValid shouldBe false
    }

    "invalid signatures" in {
      val checkpointBlock: CheckpointBlock = mock[CheckpointBlock]
      val mockHashSignature: HashSignature = mock[HashSignature]
      val mockCE: CheckpointEdge = mock[CheckpointEdge]
      val mockEdge: Edge[CheckpointEdgeData] = mock[Edge[CheckpointEdgeData]]
      val mocOE: ObservationEdge = mock[ObservationEdge]
      val mockSOE: SignedObservationEdge = mock[SignedObservationEdge]

      checkpointBlock.transactions shouldReturn Seq()
      mockHashSignature.valid(*) shouldReturn false
      checkpointBlock.signatures shouldReturn Seq(mockHashSignature, mockHashSignature)
      checkpointBlock.checkpoint shouldReturn mockCE
      mockCE.edge shouldReturn mockEdge
      mockEdge.observationEdge shouldReturn mocOE
      mocOE.hash shouldReturn "forgedOE"
      mocOE.toString shouldReturn "mockedOE"
      mockEdge.signedObservationEdge shouldReturn mockSOE
      mockSOE.hash shouldReturn "copiedSOE"
      mockSOE.toString shouldReturn "mockedSOE"

      val validationRes = cbValidator.simpleValidation(checkpointBlock)
      validationRes.unsafeRunSync.isValid shouldBe false
    }

    "invalid hash" in {
      val checkpointBlock: CheckpointBlock = mock[CheckpointBlock]
      val mockHashSignature: HashSignature = mock[HashSignature]
      val mockCE: CheckpointEdge = mock[CheckpointEdge]
      val mockEdge: Edge[CheckpointEdgeData] = mock[Edge[CheckpointEdgeData]]
      val mocOE: ObservationEdge = mock[ObservationEdge]
      val mockSOE: SignedObservationEdge = mock[SignedObservationEdge]

      checkpointBlock.transactions shouldReturn Seq()
      mockHashSignature.valid(*) shouldReturn true
      checkpointBlock.signatures shouldReturn Seq(mockHashSignature, mockHashSignature)
      checkpointBlock.checkpoint shouldReturn mockCE
      mockCE.edge shouldReturn mockEdge
      mockEdge.observationEdge shouldReturn mocOE
      mocOE.hash shouldReturn "forgedOE"
      mocOE.toString shouldReturn "mockedOE"
      mockEdge.signedObservationEdge shouldReturn mockSOE
      mockSOE.hash shouldReturn "copiedSOE"
      mockSOE.toString shouldReturn "mockedSOE"

      val validationRes = cbValidator.simpleValidation(checkpointBlock)
      validationRes.unsafeRunSync.isValid shouldBe false
    }
  }

  "validateSourceAddressBalances" - {
    val src = "DAG123"
    val preFilledTx: (Long, Option[Long]) => Transaction =
      Fixtures.manuallyCreateTransaction(src, "", "", "", Seq.empty, _, LastTransactionRef.empty, _)

    "should successfully pass validation if an address has sufficient balance regarding tx amount and fee" in {
      addressService.lookup(src) shouldReturnF AddressCacheData(5L, 0L).some
      val tx = preFilledTx(5L, None)
      val result = cbValidator.validateSourceAddressBalances(Seq(tx)).unsafeRunSync

      result shouldBe Validated.validNel(List(tx))
    }

    "should fail validation if an address has insufficient balance regarding tx amount and fee" in {
      addressService.lookup(src) shouldReturnF AddressCacheData(5L, 0L).some
      val txWithFee = preFilledTx(5L, 1L.some)
      val result = cbValidator.validateSourceAddressBalances(Seq(txWithFee)).unsafeRunSync()

      result shouldBe Validated.invalidNel(InsufficientBalance(src))
    }
  }
}
