package org.constellation

import java.security.KeyPair
import java.util.UUID

import better.files.File
import cats.effect.IO
import com.google.common.hash.Hashing
import com.typesafe.scalalogging.Logger
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointService}
import org.constellation.consensus.ConsensusRemoteSender
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.redownload.{DownloadService, RedownloadService}
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.gossip.snapshot.SnapshotProposalGossipService
import org.constellation.gossip.state.GossipMessage
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.makeKeyPair
import org.constellation.p2p.{Cluster, DataResolver, PeerData}
import org.constellation.schema.snapshot.{SnapshotProposalPayload}
import org.constellation.schema.{Id, NodeState}
import org.constellation.storage._
import org.constellation.util.Metrics
import org.http4s.metrics.prometheus.Prometheus
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}

import scala.concurrent.ExecutionContext

object TestHelpers extends IdiomaticMockito with IdiomaticMockitoCats with ArgumentMatchersSugar {

  def prepareFacilitators(size: Int): Map[Id, PeerData] =
    Seq
      .fill(size) {
        val hash = randomHash
        val facilitatorId1 = Id(hash)
        val peerData1: PeerData = mock[PeerData]
        peerData1.peerMetadata shouldReturn PeerMetadata(
          "1.2.3.4",
          9000,
          facilitatorId1,
          timeAdded = System
            .currentTimeMillis() - (ProcessingConfig().minPeerTimeAddedSeconds * 4000),
          resourceInfo = mock[ResourceInfo]
        )

        facilitatorId1 -> peerData1
      }
      .toMap

  def randomHash: String = Hashing.sha256.hashBytes(UUID.randomUUID().toString.getBytes).toString

  def prepareMockedDAO(facilitators: Map[Id, PeerData] = prepareFacilitators(1)): DAO =
    Prometheus
      .collectorRegistry[IO]
      .use { registry =>
        import constellation._

        implicit val kp: KeyPair = makeKeyPair()

        val dao: DAO = mock[DAO]

        dao.nodeConfig shouldReturn NodeConfig()

        dao.apiClient shouldReturn mock[ClientInterpreter[IO]]

        val f = File(s"tmp/${kp.getPublic.toId.medium}/db")
        f.createDirectoryIfNotExists()
        dao.dbPath shouldReturn f

        dao.id shouldReturn Fixtures.id
        dao.alias shouldReturn None

        dao.snapshotProposalGossipService shouldReturn mock[SnapshotProposalGossipService[IO]]
        dao.snapshotProposalGossipService.spread(*[SnapshotProposalPayload]) shouldReturnF Unit
        dao.snapshotProposalGossipService.spread(*[GossipMessage[SnapshotProposalPayload]]) shouldReturnF Unit

        val rds = mock[RedownloadService[IO]]
        dao.redownloadService shouldReturn rds
        dao.redownloadService.persistAcceptedSnapshot(*, *) shouldReturnF Unit
        dao.redownloadService.persistCreatedSnapshot(*, *, *) shouldReturnF Unit

        val ss = mock[SOEService[IO]]
        dao.soeService shouldReturn ss

        val ns = mock[NotificationService[IO]]
        dao.notificationService shouldReturn ns

        val ms = mock[MessageService[IO]]
        dao.messageService shouldReturn ms

        val ts = mock[TransactionService[IO]]
        dao.transactionService shouldReturn ts

        val cts = mock[ConcurrentTipService[IO]]
        dao.concurrentTipService shouldReturn cts

        val rl = mock[RateLimiting[IO]]
        dao.rateLimiting shouldReturn rl

        val rs = mock[ConsensusRemoteSender[IO]]
        dao.consensusRemoteSender shouldReturn rs

        val cs = mock[CheckpointService[IO]]
        dao.checkpointService shouldReturn cs

        val cas = mock[CheckpointAcceptanceService[IO]]
        dao.checkpointAcceptanceService shouldReturn cas

        val os = mock[ObservationService[IO]]
        dao.observationService shouldReturn os

        val snapS = mock[SnapshotService[IO]]
        dao.snapshotService shouldReturn snapS

        val keyPair = KeyUtils.makeKeyPair()
        dao.keyPair shouldReturn keyPair

        dao.cluster shouldReturn mock[Cluster[IO]]
        dao.cluster.getNodeState shouldReturn IO.pure(NodeState.Ready)

        val ba = mock[BlacklistedAddresses[IO]]
        dao.blacklistedAddresses shouldReturn ba

        dao.peerInfo shouldReturn IO.pure(Map.empty)

        val metrics = new Metrics(registry, 600, ExecutionContext.global)(dao)
        dao.metrics shouldReturn metrics

        val cluster = mock[Cluster[IO]]
        cluster.getNodeState shouldReturnF NodeState.Ready
        dao.cluster shouldReturn cluster

        val tcs = mock[TransactionChainService[IO]]
        dao.transactionChainService shouldReturn tcs

        val as = mock[AddressService[IO]]
        dao.addressService shouldReturn as

        val ds = mock[DownloadService[IO]]
        dao.downloadService shouldReturn ds

        dao.miscLogger shouldReturn Logger("miscLogger")

        dao.readyPeers shouldReturn IO.pure(facilitators)

        val dr = mock[DataResolver[IO]]
        dao.dataResolver shouldReturn dr

        val tsmmp = mock[ThreadSafeMessageMemPool]
        dao.threadSafeMessageMemPool shouldReturn tsmmp

        IO(dao)
      }
      .unsafeRunSync()

}
