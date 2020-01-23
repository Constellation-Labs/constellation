package org.constellation.p2p
import cats.effect.IO
import org.constellation.consensus.{Snapshot, SnapshotInfo}
import org.constellation.domain.snapshot.SnapshotStorage
import org.constellation.infrastructure.snapshot.SnapshotFileStorage
import org.constellation.primitives.Schema
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, TestHelpers}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContextExecutor

class DownloadProcessTest extends FunSuite with IdiomaticMockito with ArgumentMatchersSugar with Matchers {

  implicit val dao: DAO = mock[DAO]
  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.bounded
  implicit val C = IO.contextShift(ec)
  implicit val timer = IO.timer(ec)

  dao.id shouldReturn Fixtures.id

  val snapInfo: SnapshotInfo = SnapshotInfo(new Snapshot("abc", Seq.empty[String]))

  val peers: Map[Id, PeerData] = TestHelpers.prepareFacilitators(3)

  val snapshotStorage: SnapshotStorage[IO] = mock[SnapshotStorage[IO]]

  val snapshotsProcessor: SnapshotsProcessor[IO] =
    new SnapshotsProcessor[IO](SnapshotsDownloader.downloadSnapshotRandomly[IO], snapshotStorage)

  val downloader: DownloadProcess[IO] =
    new DownloadProcess(snapshotsProcessor, dao.cluster, dao.checkpointAcceptanceService, snapshotStorage)

  ignore("should get majority snapshot when most of the cluster part is responsive") {
    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.postNonBlockingArrayByteF[IO](*, *, *, *)(*)(*, *, *) shouldReturn IO
        .pure(KryoSerializer.serializeAnyRef(snapInfo))
    }
    peers.last._2.client.postNonBlockingArrayByteF[IO](*, *, *, *)(*)(*, *, *) shouldReturn
      IO.raiseError[Array[Byte]](new Exception("ups"))

    downloader.getMajoritySnapshot(peers, Seq()).unsafeRunSync() shouldBe snapInfo
  }

  test("should fail to get majority snapshot when all of the cluster members is unresponsive") {
    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.postNonBlockingArrayByteF[IO](*, *, *, *)(*)(*, *, *) shouldReturn IO
        .raiseError[Array[Byte]](new Exception("ups"))
    }

    assertThrows[Exception] {
      downloader.getMajoritySnapshot(peers, Seq()).unsafeRunSync()
    }
  }
}
