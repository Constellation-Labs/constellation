package org.constellation

import cats.effect.{ContextShift, IO}
import org.constellation.infrastructure.snapshot.{SnapshotInfoLocalStorage, SnapshotLocalStorage}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class SnapshotHashIntegrityTest extends AnyFreeSpec with Matchers {
  implicit val cc: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  "snapshot read" in {
    val storage = SnapshotLocalStorage[IO]("src/test/resources")
    val snapshot = storage.read("snapshot").rethrowT.unsafeRunSync()

    snapshot.snapshot.hash shouldBe "79cb7849a74c83dd299868305503a8852e092d8f928c703dafa20f547b7f9540"

  }

  "snapshot info read" in {
    val storage = SnapshotInfoLocalStorage[IO]("src/test/resources")
    val snapshotInfo = storage.read("snapshot-info").rethrowT.unsafeRunSync()

    snapshotInfo.snapshot.snapshot.hash shouldBe "79cb7849a74c83dd299868305503a8852e092d8f928c703dafa20f547b7f9540"

  }

}
