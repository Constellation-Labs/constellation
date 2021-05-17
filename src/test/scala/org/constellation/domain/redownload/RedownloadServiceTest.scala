package org.constellation.domain.redownload

import better.files.File
import cats.data.{EitherT, Kleisli, NonEmptyList}
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.syntax.all._
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.cloud.{CloudStorageOld, HeightHashFileStorage}
import org.constellation.domain.redownload.RedownloadService.{SnapshotProposalsAtHeight, SnapshotsAtHeight}
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.client.SnapshotClientInterpreter
import org.constellation.keytool.KeyUtils
import org.constellation.p2p.{Cluster, MajorityHeight, PeerData}
import org.constellation.rewards.RewardsManager
import org.constellation.schema.Id
import org.constellation.schema.signature.{HashSignature, Signed}
import org.constellation.schema.snapshot.{SnapshotInfo, SnapshotProposal, StoredSnapshot}
import org.constellation.storage.SnapshotService
import org.constellation.util.Metrics
import org.constellation.{PeerMetadata, ResourceInfo}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext

class RedownloadServiceTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  var cluster: Cluster[IO] = _
  var redownloadService: RedownloadService[IO] = _
  var majorityStateChooser: MajorityStateChooser = _
  var missingProposalFinder: MissingProposalFinder = _
  var snapshotStorage: LocalFileStorage[IO, StoredSnapshot] = _
  var snapshotCloudStorage: HeightHashFileStorage[IO, StoredSnapshot] = _
  var snapshotService: SnapshotService[IO] = _
  var checkpointAcceptanceService: CheckpointAcceptanceService[IO] = _
  var snapshotInfoStorage: LocalFileStorage[IO, SnapshotInfo] = _
  var snapshotInfoCloudStorage: HeightHashFileStorage[IO, SnapshotInfo] = _
  var rewardsStorage: LocalFileStorage[IO, StoredRewards] = _
  var rewardsCloudStorage: HeightHashFileStorage[IO, StoredRewards] = _
  var cloudStorage: CloudStorageOld[IO] = _
  var metrics: Metrics = _
  var rewardsManager: RewardsManager[IO] = _
  var apiClient: ClientInterpreter[IO] = _
  var cloudService: CloudServiceEnqueue[IO] = _

  val meaningfulSnapshotsCount = 4
  val redownloadInterval = 2
  val heightInterval = 2
  val keyPair = KeyUtils.makeKeyPair()

  before {
    cluster = mock[Cluster[IO]]
    // TODO: Mock methods!
    metrics = mock[Metrics]
    majorityStateChooser = mock[MajorityStateChooser]
    missingProposalFinder = mock[MissingProposalFinder]
    snapshotStorage = mock[LocalFileStorage[IO, StoredSnapshot]]
    snapshotInfoStorage = mock[LocalFileStorage[IO, SnapshotInfo]]
    rewardsStorage = mock[LocalFileStorage[IO, StoredRewards]]
    snapshotCloudStorage = mock[HeightHashFileStorage[IO, StoredSnapshot]]
    snapshotInfoCloudStorage = mock[HeightHashFileStorage[IO, SnapshotInfo]]
    rewardsCloudStorage = mock[HeightHashFileStorage[IO, StoredRewards]]
    cloudStorage = mock[CloudStorageOld[IO]]
    rewardsManager = mock[RewardsManager[IO]]
    apiClient = mock[ClientInterpreter[IO]]
    cloudService = mock[CloudServiceEnqueue[IO]]

    metrics.incrementMetricAsync[IO](*)(*) shouldReturnF Unit
    metrics.updateMetricAsync[IO](*, any[Long])(*) shouldReturnF Unit

    redownloadService = RedownloadService[IO](
      meaningfulSnapshotsCount,
      redownloadInterval,
      heightInterval,
      cluster,
      majorityStateChooser,
      missingProposalFinder,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotService,
      cloudService,
      checkpointAcceptanceService,
      rewardsManager,
      apiClient,
      keyPair,
      metrics,
      ExecutionContext.global,
      Blocker.liftExecutionContext(ExecutionContext.global)
    )
  }

  "shouldRedownload" - {
    val snapshotRedownloadHeightDelayInterval = 4

    "above" - {
      "returns true" - {
        "if reached redownload interval" in {
          val acceptedSnapshots = Map(2L -> "a", 4L -> "b", 6L -> "c", 8L -> "d")

          val majorityState = Map(2L -> "a", 4L -> "b")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }

        "if not reached redownload interval yet but already misaligned" in {
          val acceptedSnapshots = Map(2L -> "a", 4L -> "x", 6L -> "c")

          val majorityState = Map(2L -> "a", 4L -> "b")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }
      }
      "returns false" - {
        "if aligned but not reached redownload interval yet" in {
          val acceptedSnapshots = Map(2L -> "a", 4L -> "b", 6L -> "c")

          val majorityState = Map(2L -> "a", 4L -> "b")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe false
        }
      }
    }

    "below" - {
      "returns true" - {
        "if reached redownload interval" in {
          val acceptedSnapshots = Map(2L -> "a", 4L -> "b")

          val majorityState = Map(2L -> "a", 4L -> "b", 6L -> "c", 8L -> "d")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }

        "if not reached redownload interval yet but already misaligned" in {
          val acceptedSnapshots = Map(2L -> "a", 4L -> "x")

          val majorityState = Map(2L -> "a", 4L -> "b", 6L -> "c")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
        }
      }
      "returns false" - {
        "if aligned but not reached redownload interval yet" in {
          val acceptedSnapshots = Map(2L -> "a", 4L -> "b")

          val majorityState = Map(2L -> "a", 4L -> "b", 6L -> "c")

          redownloadService
            .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe false
        }
      }
    }

    "same height" - {
      "returns true if misaligned" in {
        val acceptedSnapshots = Map(2L -> "a", 4L -> "c")

        val majorityState = Map(2L -> "a", 4L -> "b")

        redownloadService
          .shouldRedownload(acceptedSnapshots, majorityState, snapshotRedownloadHeightDelayInterval) shouldBe true
      }

      "returns false if aligned" in {
        val acceptedSnapshots = Map(2L -> "a", 4L -> "b")

        val majorityState = Map(2L -> "a", 4L -> "b")

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
      val trust = SortedMap(Id("a") -> 0.2, Id("b") -> -0.4)
      val persist = redownloadService.persistCreatedSnapshot(2L, "aabbcc", trust)
      val check = redownloadService.createdSnapshots.get.map(_.get(2L))
      (persist >> check).unsafeRunSync.get should matchPattern {
        case Signed(_, SnapshotProposal("aabbcc", 2L, _)) => ()
      }
    }

    "should not override previously persisted snapshot if snapshot at given height already exists" in {
      val persistFirst = redownloadService.persistCreatedSnapshot(2L, "aaaa", SortedMap.empty)
      val persistSecond = redownloadService.persistCreatedSnapshot(2L, "bbbb", SortedMap.empty)
      val check = redownloadService.createdSnapshots.get.map(_.get(2L))

      (persistFirst >> persistSecond >> check).unsafeRunSync.get should matchPattern {
        case Signed(_, SnapshotProposal("aaaa", 2L, _)) => ()
      }
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
  }

  "removeSnapshotsAndProposalsBelowHeight" - {
    val peer1 = Id("p1")
    val peer2 = Id("p2")
    val peer1signature = HashSignature("xyz1", peer1)
    val peer2signature = HashSignature("xyz2", peer2)

    "should remove snapshots and proposals below 12 and keep all other" in {
      // given
      (for {
        _ <- redownloadService.persistAcceptedSnapshot(10, "hash10")
        _ <- redownloadService.persistAcceptedSnapshot(12, "hash12")
        _ <- redownloadService.persistCreatedSnapshot(10, "hash10", SortedMap.empty)
        _ <- redownloadService.persistCreatedSnapshot(12, "hash12", SortedMap.empty)
        _ <- redownloadService.addPeerProposal(
          Signed(peer1signature, SnapshotProposal("hash10p1", 10, SortedMap.empty))
        )
        _ <- redownloadService.addPeerProposal(
          Signed(peer1signature, SnapshotProposal("hash12p1", 12, SortedMap.empty))
        )
        _ <- redownloadService.addPeerProposal(
          Signed(peer2signature, SnapshotProposal("hash10p2", 10, SortedMap.empty))
        )
        _ <- redownloadService.addPeerProposal(
          Signed(peer2signature, SnapshotProposal("hash10p2", 12, SortedMap.empty))
        )
      } yield ()).unsafeRunSync()

      // when
      redownloadService.removeSnapshotsAndProposalsBelowHeight(12).unsafeRunSync()

      // then
      val acceptedSnapshots = redownloadService.getAcceptedSnapshots().unsafeRunSync()
      acceptedSnapshots should not contain key(10L)
      (acceptedSnapshots should contain).key(12L)

      val createdSnapshots = redownloadService.getCreatedSnapshots().unsafeRunSync()
      createdSnapshots should not contain key(10L)
      (createdSnapshots should contain).key(12L)

      val peer1Proposals = redownloadService.getPeerProposals(peer1).unsafeRunSync().getOrElse(Map.empty)
      peer1Proposals should not contain key(10L)
      (peer1Proposals should contain).key(12L)

      val peer2Proposals = redownloadService.getPeerProposals(peer2).unsafeRunSync().getOrElse(Map.empty)
      peer2Proposals should not contain key(10L)
      (peer2Proposals should contain).key(12L)
    }
  }

  "getCreatedSnapshots" - {
    "should return empty Map if there are no own snapshots" in {
      val check = redownloadService.getCreatedSnapshots()

      check.unsafeRunSync shouldBe Map.empty
    }

    "should return all own snapshots if they exist" in {
      val persistFirst = redownloadService.persistCreatedSnapshot(2L, "aaaa", SortedMap.empty)
      val persistSecond = redownloadService.persistCreatedSnapshot(4L, "bbbb", SortedMap.empty)
      val check = redownloadService.getCreatedSnapshots()

      (persistFirst >> persistSecond >> check).unsafeRunSync.toList should matchPattern {
        case List(
            (2L, Signed(_, SnapshotProposal("aaaa", 2L, _))),
            (4L, Signed(_, SnapshotProposal("bbbb", 4L, _)))
            ) =>
          ()
      }
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

  "fetchAndUpdatePeersProposals" - {
    val peer1 = Id("p1")
    val peer2 = Id("p2")
    val peer1signature = HashSignature("xyz1", peer1)
    val peer2signature = HashSignature("xyz2", peer2)
    val peer1Data = PeerData(
      PeerMetadata("host1", 9999, peer1, resourceInfo = mock[ResourceInfo]),
      NonEmptyList(mock[MajorityHeight], Nil)
    )
    val peer2Data = PeerData(
      PeerMetadata("host2", 9999, peer2, resourceInfo = mock[ResourceInfo]),
      NonEmptyList(mock[MajorityHeight], Nil)
    )
    val initialPeersProposals =
      Map(peer1 -> Map(1L -> Signed(peer1signature, SnapshotProposal("hash1p1", 1L, SortedMap.empty))))
    val peer1Proposals =
      Map(
        1L -> Signed(peer1signature, SnapshotProposal("hash2p1", 1L, SortedMap.empty)),
        2L -> Signed(peer1signature, SnapshotProposal("hash3p1", 2L, SortedMap.empty))
      )
    val peer2Proposals =
      Map(
        1L -> Signed(peer2signature, SnapshotProposal("hash1p2", 1L, SortedMap.empty)),
        2L -> Signed(peer2signature, SnapshotProposal("hash2p2", 2L, SortedMap.empty))
      )

    "for empty proposals Map" - {
      "should not override previously stored proposals" in {
        // preparing mocks and initial state
        initialPeersProposals.toList.traverse {
          case (id, proposals) =>
            redownloadService.replacePeerProposals(id, proposals)
        }.unsafeRunSync()
        cluster.getPeerInfo shouldReturnF Map(peer1 -> peer1Data)
        apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
        apiClient.snapshot.getCreatedSnapshots() shouldReturn Kleisli
          .apply[IO, PeerClientMetadata, SnapshotProposalsAtHeight] { _ =>
            IO.pure(Map.empty)
          }

        redownloadService.fetchAndUpdatePeersProposals().unsafeRunSync

        val actual = redownloadService.getAllPeerProposals().unsafeRunSync()
        val expected = initialPeersProposals

        actual shouldEqual expected
      }
    }

    "for non empty proposals Map" - {
      "should add proposals when there are no proposals stored yet" in {
        cluster.getPeerInfo shouldReturnF Map(peer1 -> peer1Data)
        apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
        apiClient.snapshot.getCreatedSnapshots() shouldReturn Kleisli
          .apply[IO, PeerClientMetadata, SnapshotProposalsAtHeight] { _ =>
            IO.pure(peer1Proposals)
          }

        redownloadService.fetchAndUpdatePeersProposals().unsafeRunSync

        val actual = redownloadService.getAllPeerProposals().unsafeRunSync()
        val expected = Map(peer1 -> peer1Proposals)

        actual shouldEqual expected
      }

      "should override peers proposals at already specified heights" in {
        initialPeersProposals.toList.traverse {
          case (id, proposals) =>
            redownloadService.replacePeerProposals(id, proposals)
        }.unsafeRunSync()
        cluster.getPeerInfo shouldReturnF Map(peer1 -> peer1Data)
        apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
        apiClient.snapshot.getCreatedSnapshots() shouldReturn Kleisli
          .apply[IO, PeerClientMetadata, SnapshotProposalsAtHeight] { _ =>
            IO.pure(peer1Proposals)
          }

        redownloadService.fetchAndUpdatePeersProposals().unsafeRunSync
        val actual = redownloadService.getAllPeerProposals().unsafeRunSync()
        val expected = Map(
          peer1 -> Map(
            1L -> Signed(peer1signature, SnapshotProposal("hash2p1", 1L, SortedMap.empty)),
            2L -> Signed(peer1signature, SnapshotProposal("hash3p1", 2L, SortedMap.empty))
          )
        )

        actual shouldEqual expected
      }

      "should add proposals for new node" in {
        initialPeersProposals.toList.traverse {
          case (id, proposals) =>
            redownloadService.replacePeerProposals(id, proposals)
        }.unsafeRunSync()
        cluster.getPeerInfo shouldReturnF Map(peer2 -> peer2Data)
        apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
        apiClient.snapshot.getCreatedSnapshots() shouldReturn Kleisli
          .apply[IO, PeerClientMetadata, SnapshotProposalsAtHeight] { _ =>
            IO.pure(peer2Proposals)
          }

        redownloadService.fetchAndUpdatePeersProposals().unsafeRunSync

        val actual = redownloadService.getAllPeerProposals().unsafeRunSync()
        val expected = initialPeersProposals ++ Map(peer2 -> peer2Proposals)

        actual shouldEqual expected
      }
    }

    /*
    "should fetch created proposals of all the peers" in {
      val peerInfo = Map(Id("node1") -> mock[PeerData], Id("node2") -> mock[PeerData])
      peerInfo.toSeq.foreach {
        case (id, peer) => {
          peer.peerMetadata shouldReturn PeerMetadata("1.2.3.4", 9001, id, resourceInfo = mock[ResourceInfo])
          apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
          apiClient.snapshot
            .getCreatedSnapshots() shouldReturn Kleisli.apply[IO, PeerClientMetadata, SnapshotProposalsAtHeight] { _ =>
            IO.pure(Map.empty)
          }
        }
      }

      cluster.getPeerInfo shouldReturnF peerInfo

      redownloadService.fetchAndUpdatePeersProposals().unsafeRunSync

      peerInfo.values.foreach { peer =>
        apiClient.snapshot.getCreatedSnapshots().was(called)
      }
    }
     */

    /*
    "should modify the local peers proposals store" in {
      val peer1 = mock[PeerData]
      val peer2 = mock[PeerData]

      val peerInfo = Map(Id("node1") -> peer1, Id("node2") -> peer2)
      val proposals = Map(2L -> "aa", 4L -> "bb")

      val ids = List(Id("node1"), Id("node2"))

      peerInfo.values.zip(ids).foreach {
        case (peer, id) =>
          apiClient.snapshot shouldReturn mock[SnapshotClientInterpreter[IO]]
          apiClient.snapshot
            .getCreatedSnapshots() shouldReturn Kleisli.apply[IO, PeerClientMetadata, SnapshotProposalsAtHeight]
             { _ =>
            IO.pure(proposals)
          }

          peer.peerMetadata shouldReturn PeerMetadata("", 1234, id, resourceInfo = null)
      }

      cluster.getPeerInfo shouldReturnF peerInfo

      val fetch = redownloadService.fetchAndUpdatePeersProposals()
      val check = redownloadService.peersProposals.get

      (fetch >> check).unsafeRunSync shouldBe Map(
        Id("node1") -> proposals,
        Id("node2") -> proposals
      )
    }
     */
    /*
    "should not fail if at least one peer did not respond" in {
      val peer1 = mock[PeerData]
      val peer2 = mock[PeerData]

      val peerInfo = Map(Id("node1") -> peer1, Id("node2") -> peer2)
      val proposals = Map(2L -> "aa", 4L -> "bb")

      peer1.client shouldReturn mock[APIClient]
      peer1.client.id shouldReturn Id("node1")
      peer1.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturnF proposals
      peer1.peerMetadata shouldReturn mock[PeerMetadata]
      peer1.peerMetadata.nodeState shouldReturn NodeState.Ready

      peer2.client shouldReturn mock[APIClient]
      peer2.client.id shouldReturn Id("node2")
      peer2.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturn IO.raiseError(
        new Throwable("error")
      )
      peer2.peerMetadata shouldReturn mock[PeerMetadata]
      peer2.peerMetadata.nodeState shouldReturn NodeState.Ready

      cluster.getPeerInfo shouldReturnF peerInfo

      val fetch = redownloadService.fetchAndUpdatePeersProposals()
      val check = redownloadService.peersProposals.get

      (fetch >> check).unsafeRunSync shouldBe Map(
        Id("node1") -> proposals,
        Id("node2") -> Map.empty
      )
    }

    "should not request offline peers" in {
      val peer1 = mock[PeerData]
      val peer2 = mock[PeerData]

      val peerInfo = Map(Id("node1") -> peer1, Id("node2") -> peer2)
      val proposals = Map(2L -> "aa", 4L -> "bb")

      peer1.client shouldReturn mock[APIClient]
      peer1.client.id shouldReturn Id("node1")
      peer1.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *) shouldReturnF proposals
      peer1.peerMetadata shouldReturn mock[PeerMetadata]
      peer1.peerMetadata.nodeState shouldReturn NodeState.Ready

      peer2.client shouldReturn mock[APIClient]
      peer2.client.id shouldReturn Id("node2")
      peer2.peerMetadata shouldReturn mock[PeerMetadata]
      peer2.peerMetadata.nodeState shouldReturn NodeState.Offline

      cluster.getPeerInfo shouldReturnF peerInfo

      val fetch = redownloadService.fetchAndUpdatePeersProposals()
      val check = redownloadService.peersProposals.get

      (fetch >> check).unsafeRunSync shouldBe Map(
        Id("node1") -> proposals
      )
      peer2.client.getNonBlockingF[IO, Map[Long, String]](*, *, *)(*)(*, *, *).wasNever(called)
    }
   */
  }
  "calculateRedownloadPlan" - {
    "above" - {
      "returns both snapshots to download and remove" in {
        val majorityState = Map(2L -> "a", 4L -> "x", 6L -> "c")

        val acceptedSnapshots =
          Map(2L -> "a", 4L -> "b", 6L -> "c", 8L -> "d", 10L -> "e", 12L -> "f")

        val diff = redownloadService.calculateRedownloadPlan(acceptedSnapshots, majorityState)
        diff.toRemove shouldEqual Map(4L -> "b", 8L -> "d", 10L -> "e", 12L -> "f")
        diff.toDownload shouldEqual Map(4L -> "x")
      }
    }

    "below" - {
      "returns both snapshots to download and remove" in {
        val acceptedSnapshots = Map(2L -> "a", 4L -> "b")

        val majorityState = Map(2L -> "a", 4L -> "x", 6L -> "c")

        val diff = redownloadService.calculateRedownloadPlan(acceptedSnapshots, majorityState)
        diff.toRemove shouldEqual Map(4L -> "b")
        diff.toDownload shouldEqual Map(4L -> "x", 6L -> "c")
      }
    }

    "same height" - {
      val acceptedSnapshots = Map(2L -> "a", 4L -> "b")

      val majorityState = Map(2L -> "a", 4L -> "x")

      "returns both snapshots to download and remove" in {
        val diff = redownloadService.calculateRedownloadPlan(acceptedSnapshots, majorityState)
        diff.toRemove shouldEqual Map(4L -> "b")
        diff.toDownload shouldEqual Map(4L -> "x")
      }
    }
  }

  /*
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
   */

  "sendMajoritySnapshotsToCloud" - {
    "if cloud storage enabled" - {
      "should upload snapshot and according snapshot info files".ignore {
        File.usingTemporaryFile() { file1 =>
          File.usingTemporaryFile() { file2 =>
            val lastMajorityState = Map(2L -> "a", 4L -> "b", 6L -> "c")
            val lastSentHeight = 4L
            println(snapshotCloudStorage)
            println(snapshotCloudStorage)

            val setMajority = redownloadService.lastMajorityState.set(lastMajorityState)
            val setLastSentHeight = redownloadService.lastSentHeight.set(lastSentHeight)

            snapshotStorage.getFile("c") shouldReturn EitherT.pure(file1)
            snapshotInfoStorage.getFile("c") shouldReturn EitherT.pure(file2)

            val check = redownloadService.sendMajoritySnapshotsToCloud()

            (setMajority >> setLastSentHeight >> check).unsafeRunSync

            snapshotCloudStorage.write(6L, "c", file1).was(called)
            snapshotInfoCloudStorage.write(6L, "c", file2).was(called)
          }
        }
      }
    }

    "if cloud storage disabled" - {
      "should do nothing" in {
        val redownloadService = RedownloadService[IO](
          meaningfulSnapshotsCount,
          redownloadInterval,
          heightInterval,
          cluster,
          majorityStateChooser,
          missingProposalFinder,
          snapshotStorage,
          snapshotInfoStorage,
          snapshotService,
          cloudService,
          checkpointAcceptanceService,
          rewardsManager,
          apiClient,
          KeyUtils.makeKeyPair(),
          metrics,
          ExecutionContext.global,
          Blocker.liftExecutionContext(ExecutionContext.global)
        )

        val check = redownloadService.sendMajoritySnapshotsToCloud()

        cloudStorage.upload(*, *).wasNever(called)
      }
    }
  }
}
