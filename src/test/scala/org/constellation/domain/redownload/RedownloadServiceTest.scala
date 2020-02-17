package org.constellation.domain.redownload

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.ConstellationExecutionContext
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.Id
import org.constellation.util.APIClient
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfterEach, FreeSpec, Matchers}

class RedownloadServiceTest
    extends FreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfterEach
    with ArgumentMatchersSugar {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  var cluster: Cluster[IO] = _
  var redownloadService: RedownloadService[IO] = _
  var majorityStateChooser: MajorityStateChooser = _

  override def beforeEach() = {
    cluster = mock[Cluster[IO]]
    majorityStateChooser = mock[MajorityStateChooser]
    redownloadService = RedownloadService[IO](cluster, majorityStateChooser)
  }

  "shouldRedownload" - {

    "returns true" - {
      "if above and reached redownload interval" in {

      }

      "if above and not reached redownload interval but already has incorrect hashes at heights anyway" in {

      }

      "if below and reached redownload interval" in {

      }

      "if below and not reached redownload interval but already has incorrect hashes at heights anyway" in {

      }

      "if at same height but has incorrect hashes at heights" in {

      }
    }

    "returns false" - {
      "if above but not reached redownload interval yet and hashes at existing heights are aligned" in {

      }

      "if below but not reached redownload interval yet and hashes at existing heights are aligned" in {

      }

      "if at same height and all the hashes are aligned" in {

      }
    }

  }

  "persistOwnSnapshot" - {
    "should persist own snapshot internally if snapshot at given height doesn't exist" in {
      val persist = redownloadService.persistCreatedSnapshot(2L, "aabbcc")
      val check = redownloadService.createdSnapshots.get.map(_.get(2L))

      (persist >> check).unsafeRunSync shouldBe "aabbcc".some
    }

    "should not override previously persisted snapshot if snapshot at given height already exists" in {
      val persistFirst = redownloadService.persistCreatedSnapshot(2L, "aaaa")
      val persistSecond = redownloadService.persistCreatedSnapshot(2L, "bbbb")
      val check = redownloadService.createdSnapshots.get.map(_.get(2L))

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe "aaaa".some
    }
  }

  "getOwnSnapshots" - {
    "should return empty map if there are no own snapshots" in {
      val check = redownloadService.getCreatedSnapshots()

      check.unsafeRunSync shouldBe Map.empty
    }

    "should return all own snapshots if they exist" in {
      val persistFirst = redownloadService.persistCreatedSnapshot(2L, "aaaa")
      val persistSecond = redownloadService.persistCreatedSnapshot(4L, "bbbb")
      val check = redownloadService.getCreatedSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe Map(2L -> "aaaa", 4L -> "bbbb")
    }
  }

  "getOwnSnapshot" - {
    "should return hash if snapshot at given height exists" in {
      val persist = redownloadService.persistCreatedSnapshot(2L, "aaaa")
      val check = redownloadService.getCreatedSnapshot(2L)

      (persist >> check).unsafeRunSync shouldBe "aaaa".some
    }

    "should return None if snapshot at given height does not exist" in {
      val check = redownloadService.getCreatedSnapshot(2L)

      check.unsafeRunSync shouldBe none[String]
    }
  }

  "fetchPeersProposals" - {
    "should fetch own proposals of all the peers" in {
      val peerInfo = Map(Id("node1") -> mock[PeerData], Id("node2") -> mock[PeerData])
      peerInfo.values.foreach { peer =>
        peer.client shouldReturn mock[APIClient]
        peer.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturnF Map.empty
      }

      cluster.getPeerInfo shouldReturnF peerInfo

      redownloadService.fetchAndUpdatePeersProposals.unsafeRunSync

      peerInfo.values.foreach { peer =>
        peer.client.getNonBlockingF[IO, Map[Long, String]]("/snapshot/own", *, *)(*)(*, *, *).was(called)
      }
    }

    "should modify the local peers proposals store" in {
      val peer1 = mock[PeerData]
      val peer2 = mock[PeerData]

      val peerInfo = Map(Id("node1") -> peer1, Id("node2") -> peer2)
      val proposals = Map(2L -> "aa", 4L -> "bb")

      val ids = List(Id("node1"), Id("node2"))

      peerInfo.values.zip(ids).foreach {
        case (peer, id) =>
          peer.client shouldReturn mock[APIClient]
          peer.client.id shouldReturn id
          peer.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturnF proposals
      }

      cluster.getPeerInfo shouldReturnF peerInfo

      val fetch = redownloadService.fetchAndUpdatePeersProposals
      val check = redownloadService.peersProposals.get

      (fetch >> check).unsafeRunSync shouldBe Map(
        Id("node1") -> proposals,
        Id("node2") -> proposals
      )
    }

    /**
      * TODO: Consider as a feature.
      * If proposals are immutable, it can be a sanity check that nodes are not changing the proposals.
      */
    "should not override previously stored proposals" ignore {}

    "should not fail if at least one peer did not respond" in {
      val peer1 = mock[PeerData]
      val peer2 = mock[PeerData]

      val peerInfo = Map(Id("node1") -> peer1, Id("node2") -> peer2)
      val proposals = Map(2L -> "aa", 4L -> "bb")

      peer1.client shouldReturn mock[APIClient]
      peer1.client.id shouldReturn Id("node1")
      peer1.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturnF proposals

      peer2.client shouldReturn mock[APIClient]
      peer2.client.id shouldReturn Id("node2")
      peer2.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturn IO.raiseError(
        new Throwable("error")
      )

      cluster.getPeerInfo shouldReturnF peerInfo

      val fetch = redownloadService.fetchAndUpdatePeersProposals
      val check = redownloadService.peersProposals.get

      (fetch >> check).unsafeRunSync shouldBe Map(
        Id("node1") -> proposals,
        Id("node2") -> Map.empty
      )
    }
  }

}
