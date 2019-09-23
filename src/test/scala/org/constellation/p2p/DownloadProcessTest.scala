package org.constellation.p2p
import cats.effect.IO
import org.constellation.consensus.{Snapshot, SnapshotInfo}
import org.constellation.primitives.Schema
import org.constellation.serializer.KryoSerializer
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, TestHelpers}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContextExecutor

class DownloadProcessTest extends FunSuite with IdiomaticMockito with ArgumentMatchersSugar with Matchers {

  implicit val dao: DAO = mock[DAO]
  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.bounded

  dao.id shouldReturn Fixtures.id

  val snapInfo: SnapshotInfo = SnapshotInfo(new Snapshot("abc", Seq.empty[String]))

  val peers: Map[Schema.Id, PeerData] = TestHelpers.prepareFacilitators(3)
  val snapshotsProcessor: SnapshotsProcessor = new SnapshotsProcessor(SnapshotsDownloader.downloadSnapshotRandomly)
  val downloader: DownloadProcess = new DownloadProcess(snapshotsProcessor)

  test("should get majority snapshot when most of the cluster part is responsive") {
    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.getNonBlockingArrayByteIO(*, *, *)(*) shouldReturn IO.pure(KryoSerializer.serializeAnyRef(snapInfo))
    }
    peers.last._2.client.getNonBlockingArrayByteIO(*, *, *)(*) shouldReturn
      IO.raiseError[Array[Byte]](new Exception("ups"))

    downloader.getMajoritySnapshot(peers).unsafeRunSync() shouldBe snapInfo
  }

  test("should fail to get majority snapshot when most of the cluster is unresponsive") {
    peers.slice(0, 2).map(_._2.client).foreach { c =>
      c.getNonBlockingArrayByteIO(*, *, *)(*) shouldReturn IO.raiseError[Array[Byte]](new Exception("ups"))
    }
    peers.last._2.client.getNonBlockingArrayByteIO(*, *, *)(*) shouldReturn
      IO.pure(KryoSerializer.serializeAnyRef(snapInfo))

    assertThrows[Exception] {
      downloader.getMajoritySnapshot(peers).unsafeRunSync()
    }
  }
}
