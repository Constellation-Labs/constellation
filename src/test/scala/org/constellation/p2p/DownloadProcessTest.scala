package org.constellation.p2p
import org.constellation.consensus.{Snapshot, SnapshotInfo}
import org.constellation.primitives.Schema
import org.constellation.{ConstellationExecutionContext, DAO, TestHelpers}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class DownloadProcessTest extends FunSuite with IdiomaticMockito with ArgumentMatchersSugar with Matchers {

  implicit val dao: DAO = mock[DAO]
  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.global

  val snapInfo: SnapshotInfo = mock[SnapshotInfo]
  snapInfo.snapshot shouldReturn mock[Snapshot]
  snapInfo.snapshot.hash shouldReturn "foo"

  val peers: Map[Schema.Id, PeerData] = TestHelpers.prepareFacilitators(3)
  val snapshotsProcessor: SnapshotsProcessor = new SnapshotsProcessor(SnapshotsDownloader.downloadSnapshotRandomly)
  val downloader: DownloadProcess = new DownloadProcess(snapshotsProcessor)

  test("should get majority snapshot when most of the cluster part is responsive") {

    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.getNonBlockingBytesKryoTry[SnapshotInfo](*, *, *) shouldReturn Future.successful(
        Success(snapInfo)
      )
    }
    peers.last._2.client.getNonBlockingBytesKryoTry[SnapshotInfo](*, *, *) shouldReturn Future.successful(
      Failure(new Exception("ups"))
    )

    downloader.getMajoritySnapshot(peers).unsafeRunSync() shouldBe snapInfo
  }

  test("should fail to get majority snapshot when most of the cluster is unresponsive") {

    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.getNonBlockingBytesKryoTry[SnapshotInfo](*, *, *) shouldReturn Future.successful(
        Failure(new Exception("ups"))
      )

    }
    peers.last._2.client.getNonBlockingBytesKryoTry[SnapshotInfo](*, *, *) shouldReturn Future.successful(
      Success(snapInfo)
    )

    assertThrows[Exception] {
      downloader.getMajoritySnapshot(peers).unsafeRunSync()
    }
  }
}
