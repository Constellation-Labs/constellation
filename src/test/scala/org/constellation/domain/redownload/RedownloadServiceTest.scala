package org.constellation.domain.redownload

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.storage.RecentSnapshot
import org.scalatest.{FreeSpec, Matchers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}

class RedownloadServiceTest extends FreeSpec with IdiomaticMockito with IdiomaticMockitoCats with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  val dao = mock[DAO]
  "persistOwnSnapshot" - {
    "should persist own snapshot internally if snapshot at given height doesn't exist" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot =  RecentSnapshot("aabbcc", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.ownSnapshots.get.map(_.get(2L))

      (persist >> check).unsafeRunSync shouldBe "aabbcc".some
    }

    "should not override previously persisted snapshot if snapshot at given height already exists" in {
      val redownloadService = RedownloadService[IO](dao)
      val firstSnapshot =  RecentSnapshot("aaaa", 2L, Map.empty)
      val secondSnapshot =  RecentSnapshot("bbbb", 2L, Map.empty)

      val persistFirst = redownloadService.persistOwnSnapshot(2L, firstSnapshot)
      val persistSecond = redownloadService.persistOwnSnapshot(2L, secondSnapshot)
      val check = redownloadService.ownSnapshots.get.map(_.get(2L))

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe "aaaa".some
    }
  }

  "getOwnSnapshots" - {
    "should return empty map if there are no own snapshots" in {
      val redownloadService = RedownloadService[IO](dao)

      val check = redownloadService.getOwnSnapshots()

      check.unsafeRunSync shouldBe Map.empty
    }

    "should return all own snapshots if they exist" in {
      val redownloadService = RedownloadService[IO](dao)
      val firstSnapshot =  RecentSnapshot("aaaa", 2L, Map.empty)
      val secondSnapshot =  RecentSnapshot("bbbb", 4L, Map.empty)
      val persistFirst = redownloadService.persistOwnSnapshot(2L, firstSnapshot)
      val persistSecond = redownloadService.persistOwnSnapshot(4L, secondSnapshot)
      val check = redownloadService.getOwnSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe Map(2L -> "aaaa", 4L -> "bbbb")
    }
  }

  "getOwnSnapshot" - {
    "should return hash if snapshot at given height exists" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot =  RecentSnapshot("aaaa", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.getOwnSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe "aaaa".some
    }

    "should return None if snapshot at given height does not exist" in {
      val redownloadService = RedownloadService[IO](dao)

      val check = redownloadService.getOwnSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

  "fetchPeersProposals" - {
    "should update peersProposals" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot =  RecentSnapshot("aaaa", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.getOwnSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe "aaaa".some
    }

    "should not update peersProposals if a new proposal is recieved" in {
      val redownloadService = RedownloadService[IO](dao)

      val check = redownloadService.getOwnSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

  "recalculateMajoritySnapshot" - {
    "should" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot =  RecentSnapshot("aaaa", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.getOwnSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe "aaaa".some
    }

    "should " in {
      val redownloadService = RedownloadService[IO](dao)

      val check = redownloadService.getOwnSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

  "checkForAlignmentWithMajoritySnapshot" - {
    "should" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot =  RecentSnapshot("aaaa", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.getOwnSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe "aaaa".some
    }

    "should " in {
      val redownloadService = RedownloadService[IO](dao)

      val check = redownloadService.getOwnSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }
}
