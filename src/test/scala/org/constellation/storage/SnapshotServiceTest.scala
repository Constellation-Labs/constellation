package org.constellation.storage

import better.files.File
import cats.effect.{ContextShift, IO, Timer}
import org.constellation._
import cats.implicits._
import org.constellation.consensus.{ConsensusManager, RandomData, Snapshot, SnapshotInfo}
import org.constellation.primitives.Schema.{CheckpointCache, Id, NodeState}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.util.Metrics
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class SnapshotServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val timer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)

  var dao: DAO = _
  var snapshotService: SnapshotService[IO] = _

  before {
    dao = mockDAO

    val cts = mock[ConcurrentTipService[IO]]
    val addressService = mock[AddressService[IO]]
    val checkpointService = mock[CheckpointService[IO]]
    val messageService = mock[MessageService[IO]]
    val transactionService = mock[TransactionService[IO]]
    val rateLimiting = mock[RateLimiting[IO]]
    val broadcastService = mock[SnapshotBroadcastService[IO]]
    val consensusManager = mock[ConsensusManager[IO]]

    snapshotService = new SnapshotService[IO](
      cts,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      rateLimiting,
      broadcastService,
      consensusManager,
      dao
    )
  }

  "exists" - {
    "should return true if snapshot hash is the latest snapshot" in {
      val lastSnapshot: Snapshot = snapshotService.snapshot.get.unsafeRunSync

      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { _ =>
          dao.snapshotPath shouldReturn dir

          snapshotService.exists(lastSnapshot.hash).unsafeRunSync shouldBe true
        }
      }
    }
  }

  "should return true if snapshot hash exists" in {
    File.usingTemporaryDirectory() { dir =>
      File.usingTemporaryFile("", "", Some(dir)) { file =>
        dao.snapshotPath shouldReturn dir

        snapshotService.exists(file.name).unsafeRunSync shouldBe true
      }
    }
  }

  "should return false if snapshot hash does not exist" in {
    File.usingTemporaryDirectory() { dir =>
      File.usingTemporaryFile("", "", Some(dir)) { _ =>
        dao.snapshotPath shouldReturn dir

        snapshotService.exists("dontexist").unsafeRunSync shouldBe false
      }
    }
  }

  "set snapshot state" - {
    "should set necessary data to perform apply function " in {
      val dao = TestHelpers.prepareRealDao()
      val snapshotService = dao.snapshotService

      val cb1 = RandomData.randomBlock(RandomData.startingTips)
      val cb2 = RandomData.randomBlock(RandomData.startingTips)
      val cbs = Seq(CheckpointCache(cb1.some, 0, None), CheckpointCache(cb2.some, 0, None))

      val snapshot = Snapshot("lastSnapHash", cbs.flatMap(_.checkpointBlock.map(_.baseHash)))
      val info: SnapshotInfo = SnapshotInfo(snapshot, snapshotCache = cbs)
      snapshotService.setSnapshot(info).unsafeRunSync()
      snapshotService.applySnapshot().unsafeRunSync()

      dao.metrics.getCountMetric(Metrics.snapshotCount) shouldBe 1.some
      dao.metrics.getCountMetric(Metrics.snapshotWriteToDisk + Metrics.success) shouldBe 1.some
    }
  }

  private def mockDAO: DAO = mock[DAO]
}
