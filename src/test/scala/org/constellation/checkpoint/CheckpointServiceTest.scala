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

    "should accept cb resolving parents soeHashes and cb baseHashes recursively" in {
      kp = makeKeyPair()
      dao = TestHelpers.prepareRealDao(readyFacilitators)

      val go = Genesis.createGenesisObservation(
        Seq(
          AccountBalance(dao.selfAddressStr, 75L + 75L + 75L)
        )
      )
      Genesis.acceptGenesis(go, setAsTips = true)

      val startingTips: Seq[SignedObservationEdge] = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

      val cb1 = makeBlock(
        startingTips,
        transactions = Seq(
          dao.transactionService
            .createTransaction(dao.selfAddressStr, Fixtures.id2.address, 75L, dao.keyPair)
            .unsafeRunSync
        )
      )

      val cb2 = makeBlock(
        startingTips,
        transactions = Seq(
          dao.transactionService
            .createTransaction(dao.selfAddressStr, Fixtures.id2.address, 75L, dao.keyPair)
            .unsafeRunSync
        )
      )
      val cb3 = makeBlock(
        Seq(cb1.soe, cb2.soe),
        transactions = Seq(
          dao.transactionService
            .createTransaction(dao.selfAddressStr, Fixtures.id2.address, 5L, dao.keyPair)
            .unsafeRunSync,
          dao.transactionService
            .createTransaction(dao.selfAddressStr, Fixtures.id2.address, 5L, dao.keyPair)
            .unsafeRunSync
        )
      )

      val peer = readyFacilitators.head._2.client
      val blocks = Seq(cb1, cb2)

      blocks.foreach { c =>
        peer.getNonBlockingIO[Option[SignedObservationEdge]](eqTo(s"soe/${c.soeHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(c.soe))

        peer.getNonBlockingIO[Option[CheckpointCache]](eqTo(s"checkpoint/${c.baseHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(CheckpointCache(c)))

        peer.postNonBlockingIO[List[(String, TransactionCacheData)]](eqTo(s"batch/transactions"), *, *)(*)(*, *) shouldReturn IO
          .pure(
            List(
              cb1.transactions
                .map(TransactionCacheData(_, cbBaseHash = Some(cb1.baseHash)))
                .map(tx => (tx.hash, tx))
                .toList ++
                cb2.transactions
                  .map(TransactionCacheData(_, cbBaseHash = Some(cb2.baseHash)))
                  .map(tx => (tx.hash, tx))
                  .toList
            ).flatten
          )
      }

      dao.checkpointAcceptanceService
        .acceptWithNodeCheck(FinishedCheckpoint(CheckpointCache(cb3, 0, Some(Height(1, 1))), Set(dao.id)))
        .unsafeRunSync()
      dao.checkpointService.contains(cb3.baseHash).unsafeRunSync() shouldBe true
    }

    "should accept cb resolving parents created with dummy transactions" in {
      kp = makeKeyPair()
      dao = TestHelpers.prepareRealDao(readyFacilitators)

      val go = Genesis.createGenesisObservation(Seq.empty)
      Genesis.acceptGenesis(go, setAsTips = true)

      val startingTips: Seq[SignedObservationEdge] = Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

      val cb1 = makeBlock(
        startingTips,
        dao.transactionService.createDummyTransactions(1).map(_.map(_.transaction)).unsafeRunSync
      )
      val cb2 = makeBlock(
        startingTips,
        dao.transactionService.createDummyTransactions(1).map(_.map(_.transaction)).unsafeRunSync
      )
      val cb3 = makeBlock(
        Seq(cb1.soe, cb2.soe),
        dao.transactionService.createDummyTransactions(1).map(_.map(_.transaction)).unsafeRunSync
      )

      val peer = readyFacilitators.head._2.client
      val blocks = Seq(cb1, cb2)

      blocks.foreach { c =>
        peer.getNonBlockingIO[Option[SignedObservationEdge]](eqTo(s"soe/${c.soeHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(c.soe))

        peer.getNonBlockingIO[Option[CheckpointCache]](eqTo(s"checkpoint/${c.baseHash}"), *, *)(*)(*, *) shouldReturn IO
          .pure(Some(CheckpointCache(c)))

        peer.postNonBlockingIO[List[(String, TransactionCacheData)]](eqTo(s"batch/transactions"), *, *)(*)(*, *) shouldReturn IO
          .pure(
            List(
              cb1.transactions
                .map(TransactionCacheData(_, cbBaseHash = Some(cb1.baseHash)))
                .map(tx => (tx.hash, tx))
                .toList ++
                cb2.transactions
                  .map(TransactionCacheData(_, cbBaseHash = Some(cb2.baseHash)))
                  .map(tx => (tx.hash, tx))
                  .toList
            ).flatten
          )
      }

      dao.checkpointAcceptanceService
        .acceptWithNodeCheck(FinishedCheckpoint(CheckpointCache(cb3, 0, Some(Height(1, 1))), Set(dao.id)))
        .unsafeRunSync()
      dao.checkpointService.contains(cb3.baseHash).unsafeRunSync() shouldBe true
    }

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
