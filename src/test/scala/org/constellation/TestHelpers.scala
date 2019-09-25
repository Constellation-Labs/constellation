package org.constellation
import java.security.KeyPair
import java.util.UUID

import better.files.File
import cats.effect.IO
import com.google.common.hash.Hashing
import com.typesafe.scalalogging.Logger
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.ConsensusRemoteSender
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils.makeKeyPair
import org.constellation.domain.configuration.NodeConfig
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.{Id, NodeState, NodeType}
import org.constellation.primitives.{ConcurrentTipService, Schema}
import org.constellation.storage._
import org.constellation.util.{APIClient, HostPort, Metrics}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats

object TestHelpers extends IdiomaticMockito with IdiomaticMockitoCats {

  def prepareRealDao(facilitators: Map[Schema.Id, PeerData] = prepareFacilitators(1)): DAO = {
    val dao: DAO = new DAO {
      override def readyPeers: IO[
        Map[Id, PeerData]
      ] = IO.pure(facilitators)
      override val keyPair = Fixtures.tempKey
    }

    dao.initialize()

    dao.metrics = {
      implicit val d: DAO = dao
      new Metrics()
    }

    dao.cluster.setNodeState(NodeState.Ready).unsafeRunSync
    dao
  }

  def prepareFacilitators(size: Int): Map[Schema.Id, PeerData] =
    Seq
      .fill(size) {
        val hash = randomHash
        val facilitatorId1 = Schema.Id(hash)
        val peerData1: PeerData = mock[PeerData]
        peerData1.peerMetadata shouldReturn mock[PeerMetadata]
        peerData1.peerMetadata.id shouldReturn facilitatorId1
        peerData1.peerMetadata.timeAdded shouldReturn System
          .currentTimeMillis() - (ProcessingConfig().minPeerTimeAddedSeconds * 4000)
        peerData1.peerMetadata.nodeType shouldReturn NodeType.Full
        peerData1.peerMetadata.nodeState shouldReturn NodeState.Ready
        peerData1.notification shouldReturn Seq()
        peerData1.client shouldReturn mock[APIClient]
        peerData1.client.hostPortForLogging shouldReturn HostPort(s"http://$hash", 9000)

        facilitatorId1 -> peerData1
      }
      .toMap

  def randomHash: String = Hashing.sha256.hashBytes(UUID.randomUUID().toString.getBytes).toString

  def prepareMockedDAO(facilitators: Map[Schema.Id, PeerData] = prepareFacilitators(1)): DAO = {
    import constellation._

    implicit val kp: KeyPair = makeKeyPair()

    val dao: DAO = mock[DAO]

    dao.nodeConfig shouldReturn NodeConfig()

    val f = File(s"tmp/${kp.getPublic.toId.medium}/db")
    f.createDirectoryIfNotExists()
    dao.dbPath shouldReturn f

    dao.id shouldReturn Fixtures.id

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

    val os = mock[ObservationService[IO]]
    dao.observationService shouldReturn os

    val keyPair = KeyUtils.makeKeyPair()
    dao.keyPair shouldReturn keyPair

    dao.cluster shouldReturn mock[Cluster[IO]]
    dao.cluster.getNodeState shouldReturn IO.pure(NodeState.Ready)

    val metrics = new Metrics(1)(dao)
    dao.metrics shouldReturn metrics

    val cluster = mock[Cluster[IO]]
    cluster.isNodeReady shouldReturnF true
    dao.cluster shouldReturn cluster

    dao.miscLogger shouldReturn Logger("miscLogger")

    dao.readyPeers shouldReturn IO.pure(facilitators)

    dao
  }

}
