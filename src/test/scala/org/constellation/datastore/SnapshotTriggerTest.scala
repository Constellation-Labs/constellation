package org.constellation.datastore

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import org.constellation.{DAO, TestHelpers}
import org.constellation.p2p.{Cluster, SetStateResult}
import org.constellation.schema.NodeState
import org.constellation.snapshot.SnapshotTrigger
import org.constellation.storage.{SnapshotCreated, SnapshotError}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext

class SnapshotTriggerTest
    extends AnyFreeSpec
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
      "calls redownload service to persist it as created snapshot" in {
        val snapshotTrigger = new SnapshotTrigger(5, ExecutionContext.global)
        dao.redownloadService.persistCreatedSnapshot(*, *, *) shouldReturnF Unit

        dao.cluster.compareAndSet(*, *, *) shouldReturnF SetStateResult(NodeState.SnapshotCreation, true)

        dao.snapshotService.attemptSnapshot() shouldReturn EitherT.pure[IO, SnapshotError](
          SnapshotCreated("aaa", 2L, Map.empty)
        )

        val trigger = snapshotTrigger.trigger()
        val cancel = snapshotTrigger.cancel()

        trigger.guarantee(cancel).unsafeRunSync
        dao.redownloadService.persistCreatedSnapshot(2L, "aaa", SortedMap.empty).was(called)
      }

      "calls redownload service to persist it as accepted snapshot" in {
        val snapshotTrigger = new SnapshotTrigger(5, ExecutionContext.global)
        dao.redownloadService.persistAcceptedSnapshot(*, *) shouldReturnF Unit

        dao.cluster.compareAndSet(*, *, *) shouldReturnF SetStateResult(NodeState.SnapshotCreation, true)

        dao.snapshotService.attemptSnapshot() shouldReturn EitherT.pure[IO, SnapshotError](
          SnapshotCreated("aaa", 2L, Map.empty)
        )

        val trigger = snapshotTrigger.trigger()
        val cancel = snapshotTrigger.cancel()

        trigger.guarantee(cancel).unsafeRunSync
        dao.redownloadService.persistAcceptedSnapshot(2L, "aaa").was(called)
      }
    }
  }
}
