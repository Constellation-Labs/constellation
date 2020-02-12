package org.constellation.domain.redownload

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.{ContextShift, IO}
import cats.implicits._
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.TestHelpers.prepareFacilitators
import org.constellation.consensus.EdgeProcessor
import org.constellation.consensus.EdgeProcessor.chunkSerialize
import org.constellation.domain.snapshotInfo.SnapshotInfoChunk
import org.constellation.p2p.{PeerAPI, PeerData}
import org.constellation.primitives.IPManager
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.{ConstellationExecutionContext, TestHelpers}
import org.json4s.native.Serialization
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RedownloadServiceTest
    extends FreeSpec
    with Matchers
    with BeforeAndAfter
    with ScalatestRouteTest
    with ArgumentMatchersSugar
    with Json4sSupport
    with IdiomaticMockito
    with IdiomaticMockitoCats {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  implicit val serialization = Serialization
  implicit val s: ActorSystem = system
  implicit val dao = TestHelpers.prepareMockedDAO()

  val numFacilitators = 10 //needs to be even for some tests below
  val facilitators
    : Map[Id, PeerData] = prepareFacilitators(numFacilitators) //todo need actual peer APIs to get responses
  val socketAddress = new InetSocketAddress("localhost", 9001)
  val ipManager: IPManager[IO] = IPManager[IO]()
  var peerAPI: PeerAPI = _

  def toRecentSnapshot(i: Int) = RecentSnapshot(i.toString, i, Map.empty)

  before {
    peerAPI = new PeerAPI(ipManager)
    dao.healthChecker.startReDownload(*, *) shouldReturn IO.pure[Unit](())
  }

  "persistOwnSnapshot" - {
    "should persist own snapshot internally if snapshot at given height doesn't exist" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot = RecentSnapshot("aabbcc", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.ownSnapshots.get.map(_.get(2L))

      (persist >> check).unsafeRunSync shouldBe RecentSnapshot("aabbcc", 2L, Map.empty).some
    }

    "should not override previously persisted snapshot if snapshot at given height already exists" in {
      val redownloadService = RedownloadService[IO](dao)
      val firstSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val secondSnapshot = RecentSnapshot("bbbb", 2L, Map.empty)

      val persistFirst = redownloadService.persistOwnSnapshot(2L, firstSnapshot)
      val persistSecond = redownloadService.persistOwnSnapshot(2L, secondSnapshot)
      val check = redownloadService.ownSnapshots.get.map(_.get(2L))

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe RecentSnapshot("aaaa", 2L, Map.empty).some
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
      val firstSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val secondSnapshot = RecentSnapshot("bbbb", 4L, Map.empty)
      val persistFirst = redownloadService.persistOwnSnapshot(2L, firstSnapshot)
      val persistSecond = redownloadService.persistOwnSnapshot(4L, secondSnapshot)
      val check = redownloadService.getOwnSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe Map(2L -> RecentSnapshot("aaaa", 2L, Map.empty),
                                                                          4L -> RecentSnapshot("bbbb", 4L, Map.empty))
    }
  }

  "getOwnSnapshot" - {
    "should return hash if snapshot at given height exists" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)

      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.getOwnSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe RecentSnapshot("aaaa", 2L, Map.empty).some
    }

    "should return None if snapshot at given height does not exist" in {
      val redownloadService = RedownloadService[IO](dao)

      val check = redownloadService.getOwnSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

  "fetchPeersProposals" - {
    val ownSnapshots = Map(0L -> RecentSnapshot("0", 0L, Map.empty),
                           2L -> RecentSnapshot("2", 2L, Map.empty),
                           4L -> RecentSnapshot("4", 4L, Map.empty),
                           6L -> RecentSnapshot("6", 6L, Map.empty))
    val serializedResponse = ownSnapshots
      .grouped(EdgeProcessor.chunkSize)
      .map(t => chunkSerialize(t.toSeq, SnapshotInfoChunk.SNAPSHOT_OWN.name))
      .toArray
    val deSer = facilitators.map { case (id, _) => RedownloadService.deSerProps((id, serializedResponse)) }.toSeq
    val proposals = deSer.flatMap { case (id, recentSnaps) => recentSnaps.map(snap => (id, snap)) }

    "should update peersProposals" in {
      val redownloadService = RedownloadService[IO](dao)
      val updateNewProps = redownloadService.updatePeerProps(proposals)

      updateNewProps.unsafeRunSync().values.forall(_.size == numFacilitators) shouldBe true
    }

    "should not update peersProposals if a new proposal at the same height as an old proposal is recieved" in {
      val redownloadService = RedownloadService[IO](dao)
      val invalidProposdals = proposals :+ (proposals.head._1, RecentSnapshot("invalidProposdals", 0L, Map.empty))
      val res = redownloadService.updatePeerProps(invalidProposdals).unsafeRunSync()

      res.values.forall(_.size == numFacilitators) shouldBe true
    }

    "should not update peersProposals with a duplicate proposal" in {
      val redownloadService = RedownloadService[IO](dao)
      val invalidProposdals = proposals :+ proposals.head
      val res = redownloadService.updatePeerProps(invalidProposdals).unsafeRunSync()
      val check = res.values.forall(_.size == numFacilitators)
      check shouldBe true
    }
  }

  "recalculateMajoritySnapshot" - {
    val ownSnapshots = Map(0L -> RecentSnapshot("0", 0L, Map.empty),
                           2L -> RecentSnapshot("2", 2L, Map.empty),
                           4L -> RecentSnapshot("4", 4L, Map.empty),
                           6L -> RecentSnapshot("6", 6L, Map.empty))
    val serializedResponse = ownSnapshots
      .grouped(EdgeProcessor.chunkSize)
      .map(t => chunkSerialize(t.toSeq, SnapshotInfoChunk.SNAPSHOT_OWN.name))
      .toArray
    val deSer = facilitators.map { case (id, _) => RedownloadService.deSerProps((id, serializedResponse)) }.toSeq
    val proposals: Seq[(Id, RecentSnapshot)] = deSer.flatMap {
      case (id, recentSnaps) => recentSnaps.map(snap => (id, snap))
    }

    "should return a majority snapshot when 50% majority achieved" in {
      val redownloadService = RedownloadService[IO](dao)
      val facilitatorDistinctSnapshots = facilitators.keysIterator
        .drop(numFacilitators / 2)
        .map { id =>
          (id, RecentSnapshot("8", 8L, Map.empty))
        }
        .toSeq
      val newProps: Seq[(Id, RecentSnapshot)] = proposals ++ facilitatorDistinctSnapshots
      val updateProps = redownloadService.updatePeerProps(newProps)
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res: (Seq[RecentSnapshot], Set[Id]) = (updateProps >> newMajority).unsafeRunSync()
      val correctSnaps = newProps.sortBy { case (id, snap) => (-snap.height, snap.hash) }.map(_._2).head
      val correctPeer = facilitatorDistinctSnapshots.map(_._1).toSet

      res._1.minBy { case snap => -snap.height } shouldBe correctSnaps
      res._2 shouldBe correctPeer
    }

    "should return correct majority snapshot when encountering non-50% split" in {
      val redownloadService = RedownloadService[IO](dao)
      val facilitatorDistinctSnapshots = facilitators.keysIterator.zipWithIndex.map {
        case (id, idx) => (id, RecentSnapshot(s"$idx", 8L, Map.empty))
      }.toSeq
      val newProps: Seq[(Id, RecentSnapshot)] = proposals ++ facilitatorDistinctSnapshots
      val updateProps = redownloadService.updatePeerProps(newProps)
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res: (Seq[RecentSnapshot], Set[Id]) = (updateProps >> newMajority).unsafeRunSync()
      val correctSnaps = newProps.sortBy { case (id, snap) => (-snap.height, snap.hash) }.map(_._2).head
      val correctPeer = newProps.sortBy { case (id, snap)  => (-snap.height, snap.hash) }.map(_._1).head

      res._1.minBy { case snap => -snap.height } shouldBe correctSnaps
      res._2 shouldBe Set(correctPeer)
    }

    "should not include an invalid snaphot when calculating new majority" in {
      val redownloadService = RedownloadService[IO](dao)
      val invalidProposdals = proposals :+ (proposals.head._1, RecentSnapshot("invalidProposdals", 0L, Map.empty))
      val updateProps = redownloadService.updatePeerProps(invalidProposdals)
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res = (updateProps >> newMajority).unsafeRunSync()
      val correctSnaps = List(6, 4, 2, 0).map(toRecentSnapshot)
      val correctIds = facilitators.keySet
      res._1 shouldBe correctSnaps
      res._2 shouldBe correctIds
    }

    "should return empty diff if not enough snaps for a majority" in {
      val redownloadService = RedownloadService[IO](dao)
      val updateProps = redownloadService.updatePeerProps(Seq())
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res = (updateProps >> newMajority).unsafeRunSync()
      res._1 shouldBe Seq()
      res._2 shouldBe Set()
    }
  }

  "checkForAlignmentWithMajoritySnapshot" - {
    val ownSnapshots = Map(0L -> RecentSnapshot("0", 0L, Map.empty),
                           2L -> RecentSnapshot("2", 2L, Map.empty),
                           4L -> RecentSnapshot("4", 4L, Map.empty),
                           6L -> RecentSnapshot("6", 6L, Map.empty))
    val serializedResponse = ownSnapshots
      .grouped(EdgeProcessor.chunkSize)
      .map(t => chunkSerialize(t.toSeq, SnapshotInfoChunk.SNAPSHOT_OWN.name))
      .toArray
    val deSer = facilitators.map { case (id, _) => RedownloadService.deSerProps((id, serializedResponse)) }.toSeq
    val proposals: Seq[(Id, RecentSnapshot)] = deSer.flatMap {
      case (id, recentSnaps) => recentSnaps.map(snap => (id, snap))
    }

    "should trigger download if should redownload" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val updateProps = redownloadService.updatePeerProps(proposals)
      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.checkForAlignmentWithMajoritySnapshot()
      val res = (updateProps >> persist >> check).unsafeRunSync()
      res shouldBe List(6, 4, 2, 0).map(toRecentSnapshot).some
    }

    "should not trigger download if aligned with majority" in {
      val redownloadService = RedownloadService[IO](dao)
      val newSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val persist = redownloadService.persistOwnSnapshot(2L, newSnapshot)
      val check = redownloadService.checkForAlignmentWithMajoritySnapshot()
      val res = (persist >> check).unsafeRunSync()
      res shouldBe None
    }
  }
}
