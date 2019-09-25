package org.constellation.consensus
import java.util.UUID

import better.files.File
import com.google.common.hash.Hashing
import org.constellation._
import org.constellation.domain.configuration.NodeConfig
import org.constellation.primitives.CheckpointBlock
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.util.Metrics
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class SnapshotTest extends FunSuite with BeforeAndAfterEach with Matchers {

  implicit val dao: DAO = new DAO
  dao.initialize(NodeConfig(primaryKeyPair = Fixtures.tempKey5))
  dao.metrics = new Metrics(200)

  test("should remove old snapshots but not recent when needed") {
    val snaps = Seq.fill(3)(
      StoredSnapshot(Snapshot(randomHash, Seq.fill(50)(randomHash)), Seq.fill(50)(CheckpointCache(Some(randomCB))))
    )

    dao.snapshotBroadcastService
      .updateRecentSnapshots(snaps.head.snapshot.hash, 2)
      .unsafeRunSync()

    Snapshot.isOverDiskCapacity(ConfigUtil.snapshotSizeDiskLimit - 4096) shouldBe false

    val paths = snaps.map(s => Snapshot.writeSnapshot(s)).map(_.isSuccess)
    paths.forall(isSuccess => isSuccess) shouldBe true

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
  private def randomCB =
    CheckpointBlock
      .createCheckpointBlockSOE(Seq.fill(10)(RandomData.randomTransaction), RandomData.startingTips)(Fixtures.tempKey1)

}
