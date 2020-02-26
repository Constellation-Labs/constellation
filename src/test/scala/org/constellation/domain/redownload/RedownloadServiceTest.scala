package org.constellation.domain.redownload

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.ConstellationExecutionContext
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.redownload.RedownloadService.SnapshotsAtHeight
import org.constellation.domain.snapshot.{SnapshotInfoStorage, SnapshotStorage}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.Id
import org.constellation.storage.SnapshotService
import org.constellation.util.{APIClient, Metrics}
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
  var snapshotStorage: SnapshotStorage[IO] = _
  var snapshotService: SnapshotService[IO] = _
  var checkpointAcceptanceService: CheckpointAcceptanceService[IO] = _
  var snapshotInfoStorage: SnapshotInfoStorage[IO] = _
  var cloudStorage: CloudStorage[IO] = _
  var metrics: Metrics = _

  val meaningfulSnapshotsCount = 4
  val redownloadInterval = 2

  override def beforeEach(): Unit = {
    cluster = mock[Cluster[IO]]
    majorityStateChooser = mock[MajorityStateChooser]
    snapshotStorage = mock[SnapshotStorage[IO]]
    snapshotInfoStorage = mock[SnapshotInfoStorage[IO]]
    cloudStorage = mock[CloudStorage[IO]]
    redownloadService = RedownloadService[IO](
      meaningfulSnapshotsCount,
      redownloadInterval,
      false,
      cluster,
      majorityStateChooser,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotService,
      checkpointAcceptanceService,
      cloudStorage,
      metrics
    )
  }

  "shouldRedownload" - {
    val snapshotRedownloadHeightDelayInterval = 4

    "above" - {
      "returns true" - {
        "if reached redownload interval" in {
          val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b", 6L -> "c", 8L -> "d")

          val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }

        "if not reached redownload interval yet but already misaligned" in {
          val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "x", 6L -> "c")

          val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }
      }
      "returns false" - {
        "if aligned but not reached redownload interval yet" in {
          val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b", 6L -> "c")

          val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe false
        }
      }
    }

    "below" - {
      "returns true" - {
        "if reached redownload interval" in {
          val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

          val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b", 6L -> "c", 8L -> "d")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }

        "if not reached redownload interval yet but already misaligned" in {
          val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "x")

          val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b", 6L -> "c")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }
      }
      "returns false" - {
        "if aligned but not reached redownload interval yet" in {
          val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

          val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b", 6L -> "c")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe false
        }
      }
    }

    "same height" - {
      "returns true if misaligned" in {
        val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "c")

        val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

        redownloadService
          .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
      }

      "returns false if aligned" in {
        val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

        val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

        redownloadService
          .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe false
      }
    }

    "empty" - {
      "returns false" in {
        val acceptedSnapshots: SnapshotsAtHeight = Map.empty

        val majorityState: SnapshotsAtHeight = Map.empty

        redownloadService
          .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe false
      }
    }
  }

  "getIgnorePoint" - {
    "is below maxHeight" in {
      val maxHeight = 30L
      val ignorePoint = redownloadService.getIgnorePoint(maxHeight)
      ignorePoint < maxHeight shouldBe true
    }

    "is above the removal point" in {
      val maxHeight = 30L
      val ignorePoint = redownloadService.getIgnorePoint(maxHeight)
      val removalPoint = redownloadService.getRemovalPoint(maxHeight)
      ignorePoint > removalPoint shouldBe true
    }
  }

  "getRemovalPoint" - {
    "is below maxHeight" in {
      val maxHeight = 30L
      val removalPoint = redownloadService.getRemovalPoint(maxHeight)
      removalPoint < maxHeight shouldBe true
    }

    "is below ignore point" in {
      val maxHeight = 30L
      val ignorePoint = redownloadService.getIgnorePoint(maxHeight)
      val removalPoint = redownloadService.getRemovalPoint(maxHeight)
      removalPoint < ignorePoint shouldBe true
    }

    "is delayed from ignorePoint by more than redownloadInterval" in {
      val maxHeight = 30L
      val ignorePoint = redownloadService.getIgnorePoint(maxHeight)
      val removalPoint = redownloadService.getRemovalPoint(maxHeight)
      removalPoint + redownloadInterval < ignorePoint shouldBe true
    }
  }

  "persistCreatedSnapshot" - {
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

    s"should limit the Map to removal point" in {
      val removalPoint = 30
      val snapshots = (1 to removalPoint + 10 by 2).map(_.toLong).toList

      val persist = snapshots.traverse(s => redownloadService.persistCreatedSnapshot(s, s.toString))
      val check =
        redownloadService.createdSnapshots.get.map(_.find { case (height, _) => height <= removalPoint }).map(_.isEmpty)

      (persist >> check).unsafeRunSync shouldBe true
    }
  }

  "persistAcceptedSnapshot" - {
    "should persist accepted snapshot internally if snapshot at given height doesn't exist" in {
      val persist = redownloadService.persistAcceptedSnapshot(2L, "aabbcc")
      val check = redownloadService.acceptedSnapshots.get.map(_.get(2L))

      (persist >> check).unsafeRunSync shouldBe "aabbcc".some
    }

    "should override previously persisted snapshot if snapshot at given height already exists" in {
      val persistFirst = redownloadService.persistAcceptedSnapshot(2L, "aaaa")
      val persistSecond = redownloadService.persistAcceptedSnapshot(2L, "bbbb")
      val check = redownloadService.acceptedSnapshots.get.map(_.get(2L))

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe "bbbb".some
    }

    s"should limit the Map to removal point" in {
      val removalPoint = 30
      val snapshots = (1 to removalPoint + 10 by 2).map(_.toLong).toList

      val persist = snapshots.traverse(s => redownloadService.persistAcceptedSnapshot(s, s.toString))
      val check =
        redownloadService.acceptedSnapshots.get
          .map(_.find { case (height, _) => height <= removalPoint })
          .map(_.isEmpty)

      (persist >> check).unsafeRunSync shouldBe true
    }
  }

  "getCreatedSnapshots" - {
    "should return empty Map if there are no own snapshots" in {
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

  "getAcceptedSnapshots" - {
    "should return empty Map if there are no accepted snapshots" in {
      val check = redownloadService.getAcceptedSnapshots()

      check.unsafeRunSync shouldBe Map.empty
    }

    "should return all accepted snapshots if they exist" in {
      val persistFirst = redownloadService.persistAcceptedSnapshot(2L, "aa")
      val persistSecond = redownloadService.persistAcceptedSnapshot(4L, "bb")
      val check = redownloadService.getAcceptedSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync shouldBe Map(2L -> "aa", 4L -> "bb")
    }
  }

  "getCreatedSnapshot" - {
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

  "fetchAndUpdatePeersProposals" - {
    "should fetch own proposals of all the peers" in {
      val peerInfo = Map(Id("node1") -> mock[PeerData], Id("node2") -> mock[PeerData])
      peerInfo.values.foreach { peer =>
        peer.client shouldReturn mock[APIClient]
        peer.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturnF Map.empty
      }

      cluster.getPeerInfo shouldReturnF peerInfo

      redownloadService.fetchAndUpdatePeersProposals().unsafeRunSync

      peerInfo.values.foreach { peer =>
        peer.client.getNonBlockingF[IO, Map[Long, String]]("snapshot/own", *, *)(*)(*, *, *).was(called)
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

      val fetch = redownloadService.fetchAndUpdatePeersProposals()
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

      val fetch = redownloadService.fetchAndUpdatePeersProposals()
      val check = redownloadService.peersProposals.get

      (fetch >> check).unsafeRunSync shouldBe Map(
        Id("node1") -> proposals,
        Id("node2") -> Map.empty
      )
    }
  }

  "calculateRedownloadPlan" - {
    "above" - {
      "returns both snapshots to download and remove" in {
        val majorityState: SnapshotsAtHeight = Map(
          2L -> "a",
          4L -> "x",
          6L -> "c"
        )

        val acceptedSnapshots: SnapshotsAtHeight = Map(
          2L -> "a",
          4L -> "b",
          6L -> "c",
          8L -> "d",
          10L -> "e",
          12L -> "f"
        )

        val diff = redownloadService.calculateRedownloadPlan(acceptedSnapshots, majorityState)
        diff.toRemove shouldEqual Map(4L -> "b", 8L -> "d", 10L -> "e", 12L -> "f")
        diff.toDownload shouldEqual Map(4L -> "x")
      }
    }

    "below" - {
      "returns both snapshots to download and remove" in {
        val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

        val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "x", 6L -> "c")

        val diff = redownloadService.calculateRedownloadPlan(acceptedSnapshots, majorityState)
        diff.toRemove shouldEqual Map(4L -> "b")
        diff.toDownload shouldEqual Map(4L -> "x", 6L -> "c")
      }
    }

    "same height" - {
      val acceptedSnapshots: SnapshotsAtHeight = Map(2L -> "a", 4L -> "b")

      val majorityState: SnapshotsAtHeight = Map(2L -> "a", 4L -> "x")

      "returns both snapshots to download and remove" in {
        val diff = redownloadService.calculateRedownloadPlan(acceptedSnapshots, majorityState)
        diff.toRemove shouldEqual Map(4L -> "b")
        diff.toDownload shouldEqual Map(4L -> "x")
      }
    }
  }

  "fetchStoredSnapshotsFromAllPeers" - {
    "should fetch stored snapshots of all peers" in {
      val peerInfo = Map(Id("node1") -> mock[PeerData], Id("node2") -> mock[PeerData])
      peerInfo.values.foreach { peer =>
        peer.client shouldReturn mock[APIClient]
        peer.client.getNonBlockingF[IO, Seq[String]](*, *, *)(*)(*, *, *) shouldReturnF Seq.empty
      }

      cluster.getPeerInfo shouldReturnF peerInfo

      redownloadService.fetchStoredSnapshotsFromAllPeers().unsafeRunSync

      peerInfo.values.foreach { peer =>
        peer.client.getNonBlockingF[IO, Seq[String]]("snapshot/stored", *, *)(*)(*, *, *).was(called)
      }
    }
  }
}
