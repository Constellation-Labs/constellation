package org.constellation.storage

import cats.implicits._
import cats.effect.{ContextShift, IO, Timer}
import org.constellation.util.HealthChecker
import org.constellation.{ConstellationExecutionContext, DAO, ProcessingConfig}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class SnapshotBroadcastServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ConstellationExecutionContext.global)

  var dao: DAO = _
  var snapshotBroadcastService: SnapshotBroadcastService[IO] = _

  before {
    dao = mockDAO
    val healthChecker = mock[HealthChecker[IO]]

    snapshotBroadcastService = new SnapshotBroadcastService[IO](
      healthChecker,
      dao
    )
  }

  "updateRecentSnapshots" - {

    "should return only recent snapshots in reversed order" in {
      dao.processingConfig shouldReturn ProcessingConfig(recentSnapshotNumber = 3)

      (1 to 4).toList
        .traverse(i => snapshotBroadcastService.updateRecentSnapshots(i.toString, 0))
        .unsafeRunSync()

      snapshotBroadcastService.getRecentSnapshots
        .unsafeRunSync()
        .map(_.hash) shouldBe List("4", "3", "2")
    }
  }

  private def mockDAO: DAO = mock[DAO]
}
