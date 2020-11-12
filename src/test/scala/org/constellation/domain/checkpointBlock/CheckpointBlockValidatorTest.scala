package org.constellation.domain.checkpointBlock

import cats.data.Validated
import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointBlockValidator, CheckpointParentService, InsufficientBalance}
import org.constellation.domain.transaction.{TransactionService, TransactionValidator}
import org.constellation.schema.v2.Schema._
import org.constellation.schema.v2.address.AddressCacheData
import org.constellation.schema.v2.checkpoint.{CheckpointBlock, CheckpointEdge, CheckpointEdgeData}
import org.constellation.schema.v2.signature.HashSignature
import org.constellation.schema.v2.edge.{Edge, ObservationEdge, SignedObservationEdge}
import org.constellation.schema.v2.transaction.{LastTransactionRef, Transaction}
import org.constellation.storage.{AddressService, SnapshotService}
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, TestHelpers}
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
  var checkpointParentService: CheckpointParentService[IO] = _
  var transactionValidator: TransactionValidator[IO] = _
  var cbValidator: CheckpointBlockValidator[IO] = _

  before {
    dao = TestHelpers.prepareMockedDAO()
    snapService = mock[SnapshotService[IO]]
    addressService = mock[AddressService[IO]]
    transactionService = mock[TransactionService[IO]]
    checkpointParentService = mock[CheckpointParentService[IO]]
    transactionValidator = mock[TransactionValidator[IO]]
    cbValidator =
      new CheckpointBlockValidator[IO](addressService, snapService, checkpointParentService, transactionValidator, dao)
    snapService.lastSnapshotHeight shouldReturn Ref.unsafe(1)
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
