package org.constellation.checkpoint

import java.security.KeyPair

import better.files.File
import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation._
import org.constellation.consensus.FinishedCheckpoint
import org.constellation.genesis.GenesisObservationWriterProperties
import org.constellation.keytool.KeyUtils.makeKeyPair
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.util.AccountBalance
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class CheckpointServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with Matchers
    with BeforeAndAfter {

  val readyFacilitators: Map[Id, PeerData] = TestHelpers.prepareFacilitators(1)

  implicit var kp: KeyPair = _
  implicit var dao: DAO = _
  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  after {
    File(GenesisObservationWriterProperties.path(dao.id), GenesisObservationWriterProperties.FILE_NAME).delete()
  }

  "with real dao" - {
    kp = makeKeyPair()
    dao = TestHelpers.prepareRealDao(readyFacilitators)

    "should store genesis observation on disk during acceptance step" in {
      Genesis.acceptGenesis(
        Genesis.createGenesisObservation(Seq.empty),
        setAsTips = true
      )

      File(GenesisObservationWriterProperties.path(dao.id), GenesisObservationWriterProperties.FILE_NAME).exists shouldBe true
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
