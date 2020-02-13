package org.constellation.datastore

import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import org.constellation.{DAO, TestHelpers}
import org.constellation.p2p.{Cluster, SetStateResult}
import org.constellation.primitives.Schema.NodeState
import org.constellation.storage.{RecentSnapshot, SnapshotCreated, SnapshotError}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class SnapshotTriggerTest
    extends FreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfter
    with ArgumentMatchersSugar {

  implicit var dao: DAO = _
  implicit var cluster: Cluster[IO] = _

  before {
    dao = TestHelpers.prepareMockedDAO()
    cluster = dao.cluster
  }

   "triggerSnapshot" - {
    "if snapshot has been created" - {
      "calls redownload service to persist own snapshot" in {
        val snapshotTrigger = new SnapshotTrigger()
        val newSnapshot =  RecentSnapshot("aaa", 2L, Map.empty)
        dao.redownloadService.persistLocalSnapshot(*) shouldReturnF Unit

        dao.cluster.compareAndSet(*, *, *) shouldReturnF SetStateResult(NodeState.SnapshotCreation, true)

        dao.snapshotService.attemptSnapshot() shouldReturn EitherT.pure[IO, SnapshotError](
          newSnapshot
        )

        val trigger = snapshotTrigger.trigger()
        val cancel = snapshotTrigger.cancel()

        (trigger >> cancel).unsafeRunSync
        dao.redownloadService.persistLocalSnapshot(newSnapshot).was(called)
      }
    }
  }
}
