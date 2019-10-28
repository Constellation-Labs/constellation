package org.constellation.checkpoint

import java.security.KeyPair

import better.files.File
import cats.effect.IO
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation._
import org.constellation.consensus.FinishedCheckpoint
import org.constellation.keytool.KeyUtils.makeKeyPair
import org.constellation.domain.schema.Id
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class CheckpointServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with Matchers
    with BeforeAndAfter {

  val readyFacilitators: Map[Id, PeerData] = TestHelpers.prepareFacilitators(1)

  implicit val kp: KeyPair = makeKeyPair()
  implicit val dao: DAO = TestHelpers.prepareRealDao(readyFacilitators)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  after {
    dao.genesisObservationPath.delete()
  }

  "with real dao" - {

    "should accept cb resolving parents soeHashes and cb baseHashes recursively" in {
      val go = Genesis.createGenesisAndInitialDistributionDirect("selfAddress", Set(dao.id), dao.keyPair)
      Genesis.acceptGenesis(go, setAsTips = true)

      val startingTips: Seq[SignedObservationEdge] = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

      val cb1 = makeBlock(
        startingTips,
        transactions = Seq(Fixtures.makeTransaction(dao.selfAddressStr, Fixtures.id2.address, 75L, dao.keyPair))
      )

      val cb2 = makeBlock(
        startingTips,
        transactions = Seq(Fixtures.makeTransaction(dao.selfAddressStr, Fixtures.id2.address, 75L, dao.keyPair))
      )
      val cb3 = makeBlock(
        Seq(cb1.soe, cb2.soe),
        transactions = Seq(Fixtures.makeTransaction(dao.selfAddressStr, Fixtures.id2.address, 75L, dao.keyPair))
      )

      val peer = readyFacilitators.head._2.client
      val blocks = Seq(cb1, cb2)

      blocks.foreach { c =>
        println(c.soeHash)
        peer.getNonBlockingIO[Option[SignedObservationEdgeCache]](eqTo(s"soe/${c.soeHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(SignedObservationEdgeCache(c.soe)))

        peer.getNonBlockingIO[Option[CheckpointCache]](eqTo(s"checkpoint/${c.baseHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(CheckpointCache(Some(c))))
      }

      dao.checkpointAcceptanceService
        .accept(FinishedCheckpoint(CheckpointCache(Some(cb3), 0, Some(Height(1, 1))), Set(dao.id)))
        .unsafeRunSync()
      dao.checkpointService.contains(cb3.baseHash).unsafeRunSync() shouldBe true
    }

    "should accept cb resolving parents created with dummy transactions" in {
      val go = Genesis.createGenesisAndInitialDistributionDirect("selfAddress", Set(dao.id), dao.keyPair)
      Genesis.acceptGenesis(go, setAsTips = true)

      val startingTips: Seq[SignedObservationEdge] = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

      val cb1 = makeBlock(startingTips)
      val cb2 = makeBlock(startingTips)
      val cb3 = makeBlock(Seq(cb1.soe, cb2.soe))

      val peer = readyFacilitators.head._2.client
      val blocks = Seq(cb1, cb2)

      blocks.foreach { c =>
        peer.getNonBlockingIO[Option[SignedObservationEdgeCache]](eqTo(s"soe/${c.soeHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(SignedObservationEdgeCache(c.soe)))

        peer.getNonBlockingIO[Option[CheckpointCache]](eqTo(s"checkpoint/${c.baseHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(CheckpointCache(Some(c))))
      }

      dao.checkpointAcceptanceService
        .accept(FinishedCheckpoint(CheckpointCache(Some(cb3), 0, Some(Height(1, 1))), Set(dao.id)))
        .unsafeRunSync()
      dao.checkpointService.contains(cb3.baseHash).unsafeRunSync() shouldBe true
    }

    "should store genesis observation on disk during acceptance step" in {
      Genesis.acceptGenesis(
        Genesis.createGenesisAndInitialDistributionDirect("selfAddress", Set(dao.id), dao.keyPair),
        setAsTips = true
      )

      File(dao.genesisObservationPath, GenesisObservationWriter.FILE_NAME).exists shouldBe true
    }
  }

  private def makeBlock(
    tips: Seq[SignedObservationEdge],
    transactions: Seq[Transaction] = Seq(
      Fixtures.makeDummyTransaction(dao.selfAddressStr, dao.dummyAddress, dao.keyPair)
    )
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
