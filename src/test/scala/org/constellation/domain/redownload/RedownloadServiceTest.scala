package org.constellation.domain.redownload

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.{ContextShift, IO}
import cats.implicits._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.Fixtures.{toRecentSnapshot}
import org.constellation.TestHelpers.prepareFacilitators
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.IPManager
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.serializer.KryoSerializer.chunkSerialize
import org.constellation.storage.RecentSnapshot
import org.constellation.util.{HealthChecker, SnapshotDiff}
import org.constellation.{ConstellationExecutionContext, TestHelpers}
import org.json4s.native.Serialization
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.concurrent.TimeoutException

class RedownloadServiceTest
    extends FreeSpec
    with Matchers
    with BeforeAndAfterAll
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
  val facilitators = prepareFacilitators(numFacilitators)
  val ownPeerInfo = prepareFacilitators(1)
  val socketAddress = new InetSocketAddress("localhost", 9001)
  val ipManager: IPManager[IO] = IPManager[IO]()

  val baseSnapshots = Map(0L -> RecentSnapshot("0", 0L, Map.empty),
                          2L -> RecentSnapshot("2", 2L, Map.empty),
                          4L -> RecentSnapshot("4", 4L, Map.empty),
                          6L -> RecentSnapshot("6", 6L, Map.empty))

  val serializedResponse: Array[Array[Byte]] = baseSnapshots
    .grouped(KryoSerializer.chunkSize)
    .map(t => chunkSerialize(t.toSeq, RedownloadService.fetchSnapshotProposals))
    .toArray
  val deSer = facilitators.map { case (id, _) => RedownloadService.deserializeProposals((id, serializedResponse)) }.toSeq
  val proposals = deSer.flatMap { case (id, recentSnaps) => recentSnaps.map(snap => (id, snap)) }

  val cluster = mock[Cluster[IO]]
  val healthChecker = mock[HealthChecker[IO]]

  override def beforeAll(): Unit = {
    healthChecker.startReDownload(*, *) shouldReturn IO.pure[Unit](())
    cluster.id shouldReturn ownPeerInfo.keySet.head
    cluster.getPeerInfo shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
  }

  "persistOwnSnapshot" - {
    "should persist own snapshot internally if snapshot at given height doesn't exist" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val newSnapshot = RecentSnapshot("aabbcc", 2L, Map.empty)

      val persist = redownloadService.persistLocalSnapshot(newSnapshot)
      val check = redownloadService.getLocalSnapshotAtHeight(2L)

      (persist >> check).unsafeRunSync shouldBe RecentSnapshot("aabbcc", 2L, Map.empty).some
    }

    "should not override previously persisted snapshot if snapshot at given height already exists" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val firstSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val secondSnapshot = RecentSnapshot("bbbb", 2L, Map.empty)

      val persistFirst = redownloadService.persistLocalSnapshot(firstSnapshot)
      val persistSecond = redownloadService.persistLocalSnapshot(secondSnapshot)
      val check = redownloadService.getLocalSnapshotAtHeight(2L)

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe RecentSnapshot("aaaa", 2L, Map.empty).some
    }
  }

  "getOwnSnapshots" - {
    "should return empty map if there are no own snapshots" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)

      val check = redownloadService.getLocalSnapshots()

      check.unsafeRunSync shouldBe Map.empty
    }

    "should return all own snapshots if they exist" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val firstSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val secondSnapshot = RecentSnapshot("bbbb", 4L, Map.empty)
      val persistFirst = redownloadService.persistLocalSnapshot(firstSnapshot)
      val persistSecond = redownloadService.persistLocalSnapshot(secondSnapshot)
      val check = redownloadService.getLocalSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe Map(2L -> RecentSnapshot("aaaa", 2L, Map.empty),
                                                                          4L -> RecentSnapshot("bbbb", 4L, Map.empty))
    }
  }

  "getOwnSnapshot" - {
    "should return hash if snapshot at given height exists" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val newSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)

      val persist = redownloadService.persistLocalSnapshot(newSnapshot)
      val check = redownloadService.getLocalSnapshotAtHeight(2L)

      (persist >> check).unsafeRunSync shouldBe RecentSnapshot("aaaa", 2L, Map.empty).some
    }

    "should return None if snapshot at given height does not exist" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)

      val check = redownloadService.getLocalSnapshotAtHeight(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

  "fetchAndSetPeersProposals" - {
    "should fetch peer proposals and modify proposal map" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (_, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse
      }
      val fetch = redownloadService.fetchAndSetPeerProposals()
      val check = redownloadService.proposedSnapshots.get
      (fetch >> check).unsafeRunSync().keySet shouldBe facilitators.keySet
    }

    "should not fail if at least one peer did not respond" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.tail.foreach {
        case (_, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse
      }
      facilitators.head._2.client.getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturn IO
        .raiseError(new TimeoutException("Testing timeout, case just ignore this message."))
      val fetch = redownloadService.fetchAndSetPeerProposals()
      val check = redownloadService.proposedSnapshots.get
      (fetch >> check).unsafeRunSync().keySet shouldBe facilitators.tail.keySet
    }
  }

  "updatePeerProposals" - {
    "should update peersProposals" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (_, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse
      }
      val fetch = redownloadService.fetchAndSetPeerProposals()
      val check = redownloadService.proposedSnapshots.get
      val allResponsesUpdated = (fetch >> check).unsafeRunSync().forall { case (id, proposals) => baseSnapshots.keySet.diff(proposals.keySet).isEmpty}
      allResponsesUpdated shouldBe true
    }

    "should not update peersProposals if a new proposal at the same height as an old proposal is recieved" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val invalidProposalHash = "invalidProposal"
      val invalidPeer = ownPeerInfo
      val invalidPeerId = invalidPeer.head._1
      val invalidProposal = RecentSnapshot(invalidProposalHash, 0L, Map.empty)
      val invalidProposals = Map(invalidPeerId -> invalidProposal)
      val serializedInvalidResponse = invalidProposals
        .grouped(KryoSerializer.chunkSize)
        .map(t => chunkSerialize(t.toSeq, RedownloadService.fetchSnapshotProposals))
        .toArray
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators ++ invalidPeer)
      facilitators.foreach {
        case (_, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse
      }
      invalidPeer.foreach {
        case (_, peerApi) =>
          (peerApi.client.getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedInvalidResponse)
            .andThen(serializedResponse)
      }
      val fetch = redownloadService.fetchAndSetPeerProposals()
      val update = redownloadService.proposedSnapshots.get
      val fetchInvalid = redownloadService.fetchAndSetPeerProposals()
      val check = redownloadService.proposedSnapshots.get
      val res = (fetch >> update >> fetchInvalid >> check).unsafeRunSync()
      val allResponsesUpdated = res.forall { case (id, proposals) => baseSnapshots.keySet.diff(proposals.keySet).isEmpty}

      res(invalidPeerId).get(0) shouldBe Some(invalidProposal)
      allResponsesUpdated shouldBe true
    }

    "should not update peersProposals with a duplicate proposal" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (_, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse
      }
      val fetch = redownloadService.fetchAndSetPeerProposals()
      val update = redownloadService.proposedSnapshots.get
      val fetchAgain = redownloadService.fetchAndSetPeerProposals()
      val check = redownloadService.proposedSnapshots.get
      val res = (fetch >> update >> fetchAgain >> check).unsafeRunSync()
      val allResponsesUpdated = res.forall { case (id, proposals) => baseSnapshots.keySet.diff(proposals.keySet).isEmpty}
      allResponsesUpdated shouldBe true
    }
  }

  "recalculateMajoritySnapshot" - {
    "should return a majority snapshot when simple majority achieved" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val (peersWithnNewSnap, _) = facilitators.keySet.splitAt(numFacilitators / 2)
      val newMajorityProposals = proposals ++ peersWithnNewSnap.map(id => (id, RecentSnapshot("8", 8L, Map.empty)))
      val idToSerializedResponse = facilitators.keys.map { id =>
        val newSnap = peersWithnNewSnap.contains(id)
        val newProposal = if (newSnap) baseSnapshots + (8L -> RecentSnapshot("8", 8L, Map.empty)) else baseSnapshots
        val newSerializedResponse = newProposal
          .grouped(KryoSerializer.chunkSize)
          .map(t => chunkSerialize(t.toSeq, RedownloadService.fetchSnapshotProposals))
          .toArray
        id -> newSerializedResponse
      }.toMap
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (id, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF idToSerializedResponse(
            id
          )
      }
      val updateProps = redownloadService.fetchAndSetPeerProposals()
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res = (updateProps >> newMajority).unsafeRunSync()

      val correctSnaps = newMajorityProposals.sortBy { case (id, snap) => (-snap.height, snap.hash) }.map(_._2).head
      val correctPeer = newMajorityProposals.sortBy { case (id, snap)  => (-snap.height, snap.hash) }.map(_._1).head
      res._1.minBy { case snap => -snap.height } shouldBe correctSnaps
      res._2 shouldBe peersWithnNewSnap
    }

    "should return correct majority snapshot when encountering distinct proposals at a given height" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val newSetOfPeerSnaps = facilitators.zipWithIndex.map {
        case ((id, peerInfo), idx) => (id, RecentSnapshot(s"$idx", 8L, Map.empty))
      }
      val idToSerializedResponse = facilitators.keys.map { id =>
        val newProposal = baseSnapshots + (8L -> newSetOfPeerSnaps(id))
        val newSerializedResponse = newProposal
          .grouped(KryoSerializer.chunkSize)
          .map(t => chunkSerialize(t.toSeq, RedownloadService.fetchSnapshotProposals))
          .toArray
        id -> newSerializedResponse
      }.toMap
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (id, peerApi) =>
          (peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse)
            .andThen(idToSerializedResponse(id))
      }
      val newProps = proposals ++ newSetOfPeerSnaps.toSeq
      val updateProposals = redownloadService.fetchAndSetPeerProposals()
      val oldMajority = redownloadService.recalculateMajoritySnapshot()
      val updateProposalsAgain = redownloadService.fetchAndSetPeerProposals()
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res = (updateProposals >> oldMajority >> updateProposalsAgain >> newMajority).unsafeRunSync()

      val correctNewSnaps = newProps.sortBy { case (id, snap) => (-snap.height, snap.hash) }.map(_._2).head
      val correctNewPeer = newProps.sortBy { case (id, snap)  => (-snap.height, snap.hash) }.map(_._1).head
      res._1.minBy { case snap => -snap.height } shouldBe correctNewSnaps
      res._2 shouldBe Set(correctNewPeer)
    }

    "should return empty diff if not enough snaps for a majority" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (id, peerApi) =>
          peerApi.client.getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF Array()
      }
      val updateProps = redownloadService.fetchAndSetPeerProposals()
      val newMajority = redownloadService.recalculateMajoritySnapshot()
      val res = (updateProps >> newMajority).unsafeRunSync()
      res._1 shouldBe Seq()
      res._2 shouldBe Set()
    }
  }

  "checkForAlignmentWithMajoritySnapshot" - {
    "should trigger download if should redownload" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val newSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      cluster.readyPeers shouldReturn IO.pure[Map[Id, PeerData]](facilitators)
      facilitators.foreach {
        case (_, peerApi) =>
          peerApi.client
            .getNonBlockingFLogged[IO, Array[Array[Byte]]](*, *, *, *)(*)(*, *, *) shouldReturnF serializedResponse
      }
      val updateProps = redownloadService.fetchAndSetPeerProposals()
      val persist = redownloadService.persistLocalSnapshot(newSnapshot)
      val check = redownloadService.checkForAlignmentWithMajoritySnapshot()
      val res = (updateProps >> persist >> check).unsafeRunSync()
      res shouldBe List(6, 4, 2, 0).map(toRecentSnapshot).some
    }

    "should not trigger download if aligned with majority" in {
      val redownloadService = RedownloadService[IO](cluster, healthChecker)
      val newSnapshot = RecentSnapshot("aaaa", 2L, Map.empty)
      val persist = redownloadService.persistLocalSnapshot(newSnapshot)
      val check = redownloadService.checkForAlignmentWithMajoritySnapshot()
      val res = (persist >> check).unsafeRunSync()
      res shouldBe None
    }
  }

  "shouldReDownload" - {
    val height = 2
    val ownSnapshots = List(height).map(i => RecentSnapshot(s"$i", i, Map.empty))
    val interval = RedownloadService.snapshotHeightRedownloadDelayInterval

    "should return true when there are snaps to delete and to download" - {
      val diff =
        SnapshotDiff(
          List(RecentSnapshot("someSnap", height, Map.empty)),
          List(RecentSnapshot("someSnap", height, Map.empty)),
          List(Id("peer"))
        )

      RedownloadService.shouldReDownload(ownSnapshots, diff) shouldBe true
    }
    "should return true when there are snaps to delete and nothing to download" - {
      val diff =
        SnapshotDiff(List(RecentSnapshot("someSnap", height, Map.empty)), List.empty, List(Id("peer")))

      RedownloadService.shouldReDownload(ownSnapshots, diff) shouldBe false
    }

    "should return false when height is too small" - {
      val diff =
        SnapshotDiff(List.empty, List(RecentSnapshot(height.toString, height, Map.empty)), List(Id("peer")))

      RedownloadService.shouldReDownload(ownSnapshots, diff) shouldBe false
    }

    "should return true when height below interval" - {
      val diff =
        SnapshotDiff(List.empty, List(RecentSnapshot("someSnap", height + (interval * 2), Map.empty)), List(Id("peer")))
      RedownloadService.shouldReDownload(ownSnapshots, diff) shouldBe true
    }
  }
}
