package org.constellation.consensus

import java.util.UUID

import better.files.File
import cats.effect.{ContextShift, IO}
import com.google.common.hash.Hashing
import cats.implicits._
import org.constellation._
import org.constellation.domain.configuration.NodeConfig
import org.constellation.metrics.Metrics
import org.constellation.primitives.CheckpointBlock
import org.constellation.primitives.Schema.{CheckpointCache, GenesisObservation}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}

class SnapshotTest extends FunSuite with BeforeAndAfter with Matchers with IdiomaticMockito
  with IdiomaticMockitoCats {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit var dao: DAO = _

  def randomHash = Hashing.sha256.hashBytes(UUID.randomUUID().toString.getBytes).toString
  var genesis: GenesisObservation = _

  def randomCB =
    CheckpointBlock
      .createCheckpointBlockSOE(Seq.fill(10)(RandomData.randomTransaction), RandomData.startingTips(genesis))(
        Fixtures.tempKey1
      )

  before {
    dao = TestHelpers.prepareRealDao(
      nodeConfig =
        NodeConfig(primaryKeyPair = Fixtures.tempKey5, processingConfig = ProcessingConfig(metricCheckInterval = 200))
    )
    File(dao.snapshotPath).clear()
    genesis = RandomData.go()
  }

  after {
    File(dao.snapshotPath).delete()
    dao.unsafeShutdown()
  }

  test("should remove snapshot distinctly and suppress not found messages") {
    val ss =
      StoredSnapshot(Snapshot(randomHash, Seq.fill(50)(randomHash)), Seq.fill(50)(CheckpointCache(randomCB)))
    Snapshot.writeSnapshot[IO](ss).value.unsafeRunSync()
    Snapshot
      .removeSnapshots[IO](List(ss.snapshot.hash, ss.snapshot.hash))
      .unsafeRunSync()
  }

  test("should remove old snapshots but not recent when needed") {
    val snaps = List.fill(3)(
      StoredSnapshot(Snapshot(randomHash, Seq.fill(50)(randomHash)), Seq.fill(50)(CheckpointCache(randomCB)))
    )

    dao.snapshotBroadcastService
      .updateRecentSnapshots(snaps.head.snapshot.hash, 2, Map.empty)
      .unsafeRunSync()

    Snapshot.isOverDiskCapacity(ConfigUtil.snapshotSizeDiskLimit - 4096).unsafeRunSync shouldBe false

    snaps.traverse(Snapshot.writeSnapshot[IO]).value.unsafeRunSync

    File(dao.snapshotPath, snaps.head.snapshot.hash).exists shouldBe true
    File(dao.snapshotPath, snaps.last.snapshot.hash).exists shouldBe true
  }
}
