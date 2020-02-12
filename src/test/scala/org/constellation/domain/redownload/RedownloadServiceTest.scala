package org.constellation.domain.redownload

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.ConstellationExecutionContext
import org.scalatest.{FreeSpec, Matchers}

class RedownloadServiceTest extends FreeSpec with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  "persistOwnSnapshot" - {
    "should persist own snapshot internally if snapshot at given height doesn't exist" in {
      val redownloadService = RedownloadService[IO]()

      val persist = redownloadService.persistOwnSnapshot(2L, "aabbcc")
      val check = redownloadService.ownSnapshots.get.map(_.get(2L))

      (persist >> check).unsafeRunSync shouldBe "aabbcc".some
    }

    "should not override previously persisted snapshot if snapshot at given height already exists" in {
      val redownloadService = RedownloadService[IO]()

      val persistFirst = redownloadService.persistOwnSnapshot(2L, "aaaa")
      val persistSecond = redownloadService.persistOwnSnapshot(2L, "bbbb")
      val check = redownloadService.ownSnapshots.get.map(_.get(2L))

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe "aaaa".some
    }
  }

  "getOwnSnapshots" - {
    "should return empty map if there are no own snapshots" in {
      val redownloadService = RedownloadService[IO]()

      val check = redownloadService.getOwnSnapshots()

      check.unsafeRunSync shouldBe Map.empty
    }

    "should return all own snapshots if they exist" in {
      val redownloadService = RedownloadService[IO]()

      val persistFirst = redownloadService.persistOwnSnapshot(2L, "aaaa")
      val persistSecond = redownloadService.persistOwnSnapshot(4L, "bbbb")
      val check = redownloadService.getOwnSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe Map(2L -> "aaaa", 4L -> "bbbb")
    }
  }

  "getOwnSnapshot" - {
    "should return hash if snapshot at given height exists" in {
      val redownloadService = RedownloadService[IO]()

      val persist = redownloadService.persistOwnSnapshot(2L, "aaaa")
      val check = redownloadService.getOwnSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe "aaaa".some
    }

    "should return None if snapshot at given height does not exist" in {
      val redownloadService = RedownloadService[IO]()

      val check = redownloadService.getOwnSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

}
