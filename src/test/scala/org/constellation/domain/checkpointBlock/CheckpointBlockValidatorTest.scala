package org.constellation.domain.checkpointBlock
import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointBlockValidator, CheckpointParentService}
import org.constellation.domain.transaction.{TransactionService, TransactionValidator}
import org.constellation.primitives.Schema._
import org.constellation.primitives.{CheckpointBlock, Edge}
import org.constellation.storage.{AddressService, SnapshotService}
import org.constellation.util.HashSignature
import org.constellation.{ConstellationExecutionContext, DAO, TestHelpers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class CheckpointBlockValidatorTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
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
}
