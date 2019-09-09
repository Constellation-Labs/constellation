package org.constellation.p2p
import cats.effect.IO
import org.constellation.consensus.{Snapshot, SnapshotInfo}
import org.constellation.primitives.Schema
import org.constellation.{ConstellationExecutionContext, DAO, TestHelpers}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class DownloadProcessTest extends FunSuite with IdiomaticMockito with ArgumentMatchersSugar with Matchers {

  implicit val dao: DAO = mock[DAO]
  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.bounded

  val snapInfo: SnapshotInfo = mock[SnapshotInfo]
  snapInfo.snapshot shouldReturn mock[Snapshot]
  snapInfo.snapshot.hash shouldReturn "foo"

  val peers: Map[Schema.Id, PeerData] = TestHelpers.prepareFacilitators(3)
  val snapshotsProcessor: SnapshotsProcessor = new SnapshotsProcessor(SnapshotsDownloader.downloadSnapshotRandomly)
  val downloader: DownloadProcess = new DownloadProcess(snapshotsProcessor)

  test("should get majority snapshot when most of the cluster part is responsive") {

    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.getNonBlockingBytesKryo[SnapshotInfo](*, *, *)(*) shouldReturn IO.pure(snapInfo)
    }
    peers.last._2.client.getNonBlockingBytesKryo[SnapshotInfo](*, *, *)(*) shouldReturn IO.raiseError[SnapshotInfo](
      new Exception("ups")
    )

    downloader.getMajoritySnapshot(peers).unsafeRunSync() shouldBe snapInfo
  }

  test("should fail to get majority snapshot when most of the cluster is unresponsive") {

    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.getNonBlockingBytesKryo[SnapshotInfo](*, *, *)(*) shouldReturn IO.raiseError[SnapshotInfo](new Exception("ups"))

    }
    peers.last._2.client.getNonBlockingBytesKryo[SnapshotInfo](*, *, *)(*) shouldReturn IO.pure(snapInfo)

    assertThrows[Exception] {
      downloader.getMajoritySnapshot(peers).unsafeRunSync()
    }
  }
}
