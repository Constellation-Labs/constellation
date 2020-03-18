package org.constellation.checkpoint

import java.security.KeyPair

import better.files.File
import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation._
import org.constellation.consensus.FinishedCheckpoint
import org.constellation.keytool.KeyUtils.makeKeyPair
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.util.AccountBalance
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FreeSpec, Matchers}

class CheckpointServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val readyFacilitators: Map[Id, PeerData] = TestHelpers.prepareFacilitators(1)

  implicit var kp: KeyPair = _
  implicit var dao: DAO = _
  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  before {
    kp = makeKeyPair()
    dao = TestHelpers.prepareRealDao(readyFacilitators)
  }

  after {
    File(s"${dao.genesisObservationPath}/genesisObservation").delete()
    dao.unsafeShutdown()
  }

  "with real dao" - {
    "should store genesis observation on disk during acceptance step" in {
      Genesis.acceptGenesis(
        Genesis.createGenesisObservation(Seq.empty),
        setAsTips = true
      )

      File(s"${dao.genesisObservationPath}/genesisObservation").exists shouldBe true
    }
  }

  private def makeBlock(
    tips: Seq[SignedObservationEdge],
    transactions: Seq[Transaction]
  ): CheckpointBlock =
    CheckpointBlock.createCheckpointBlock(
      transactions,
      tips.map { s =>
        TypedEdgeHash(s.hash, EdgeHashType.CheckpointHash)
      },
      Seq(),
      Seq()
    )

}
