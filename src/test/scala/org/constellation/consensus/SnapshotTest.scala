package org.constellation.consensus

import java.util.UUID

import better.files.File
import cats.effect.{ContextShift, IO}
import com.google.common.hash.Hashing
import cats.implicits._
import org.constellation._
import org.constellation.domain.configuration.NodeConfig
import org.constellation.primitives.CheckpointBlock
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.util.Metrics
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class SnapshotTest extends FunSuite with BeforeAndAfterEach with Matchers {

  implicit val dao: DAO = TestHelpers.prepareRealDao(
    nodeConfig =
      NodeConfig(primaryKeyPair = Fixtures.tempKey5, processingConfig = ProcessingConfig(metricCheckInterval = 200))
  )
  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  test("should remove snapshot distinctly and suppress not found messages") {
    val ss =
      StoredSnapshot(Snapshot(randomHash, Seq.fill(50)(randomHash)), Seq.fill(50)(CheckpointCache(randomCB)))
    Snapshot.writeSnapshot[IO](ss).value.unsafeRunSync()
    Snapshot
      .removeSnapshots[IO](List(ss.snapshot.hash, ss.snapshot.hash), dao.snapshotPath.pathAsString)
      .unsafeRunSync()
  }

  test("should remove old snapshots but not recent when needed") {
    val snaps = List.fill(3)(
      StoredSnapshot(Snapshot(randomHash, Seq.fill(50)(randomHash)), Seq.fill(50)(CheckpointCache(randomCB)))
    )

    dao.snapshotBroadcastService
      .updateRecentSnapshots(snaps.head.snapshot.hash, 2, Map.empty)
      .unsafeRunSync()

    Snapshot.isOverDiskCapacity(ConfigUtil.snapshotSizeDiskLimit - 4096) shouldBe false

    snaps.traverse(Snapshot.writeSnapshot[IO]).value.unsafeRunSync

    File(dao.snapshotPath.path, snaps.head.snapshot.hash).exists shouldBe true
    File(dao.snapshotPath.path, snaps.last.snapshot.hash).exists shouldBe true
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    dao.snapshotPath.delete()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    dao.snapshotPath.delete()
  }

  private def randomHash = Hashing.sha256.hashBytes(UUID.randomUUID().toString.getBytes).toString
  private val genesis = RandomData.go()
  private def randomCB =
    CheckpointBlock
      .createCheckpointBlockSOE(Seq.fill(10)(RandomData.randomTransaction), RandomData.startingTips(genesis))(
        Fixtures.tempKey1
      )

}
