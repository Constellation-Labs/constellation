package org.constellation

import java.security.KeyPair
import java.util.UUID

import better.files.File
import cats.effect.IO
import cats.implicits._
import com.google.common.hash.Hashing
import com.typesafe.scalalogging.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointService}
import org.constellation.consensus.ConsensusRemoteSender
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.makeKeyPair
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.redownload.RedownloadService
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.p2p.{Cluster, JoiningPeerValidator, PeerData}
import org.constellation.primitives.{ConcurrentTipService, IPManager}
import org.constellation.primitives.Schema.{NodeState, NodeType}
import org.constellation.schema.Id
import org.constellation.storage._
import org.constellation.trust.TrustManager
import org.constellation.util.{APIClient, HealthChecker, HostPort, Metrics}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats

object TestHelpers extends IdiomaticMockito with IdiomaticMockitoCats {

  def prepareRealDao(
    facilitators: Map[Id, PeerData] = prepareFacilitators(1),
    nodeConfig: NodeConfig = NodeConfig()
  ): DAO = {
    val dao: DAO = new DAO {
      override def readyPeers: IO[
        Map[Id, PeerData]
      ] = IO.pure(facilitators)

      override def peerInfo: IO[
        Map[Id, PeerData]
      ] = IO.pure(facilitators)

      override def registerAgent(id: Id): IO[Unit] = IO.unit
    }

    dao.nodeConfig = nodeConfig
    dao.metrics = new Metrics(nodeConfig.processingConfig.metricCheckInterval)(dao)
    dao.initialize(nodeConfig)

    (dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready) >>
      facilitators.toList.traverse(p => dao.cluster.updatePeerInfo(p._2))).unsafeRunSync
    dao
  }

  def prepareFacilitators(size: Int): Map[Id, PeerData] =
    Seq
      .fill(size) {
        val hash = randomHash
        val facilitatorId1 = Id(hash)
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
        peerData1.client.id shouldReturn facilitatorId1

        facilitatorId1 -> peerData1
      }
      .toMap

  def randomHash: String = Hashing.sha256.hashBytes(UUID.randomUUID().toString.getBytes).toString

  def prepareMockedDAO(facilitators: Map[Id, PeerData] = prepareFacilitators(1)): DAO = {
    import constellation._

    implicit val kp: KeyPair = makeKeyPair()

    val dao: DAO = mock[DAO]

    dao.nodeConfig shouldReturn NodeConfig()

    dao.id shouldReturn Fixtures.id

    val rds = mock[RedownloadService[IO]]
    dao.redownloadService shouldReturn rds

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

    val hc = mock[HealthChecker[IO]]
    dao.healthChecker shouldReturn hc

    val keyPair = KeyUtils.makeKeyPair()
    dao.keyPair shouldReturn keyPair

    dao.cluster shouldReturn mock[Cluster[IO]]
    dao.cluster.getNodeState shouldReturn IO.pure(NodeState.Ready)

    val metrics = new Metrics(1)(dao)
    dao.metrics shouldReturn metrics

    val cluster = mock[Cluster[IO]]
    cluster.getNodeState shouldReturnF NodeState.Ready
    dao.cluster shouldReturn cluster

    dao.miscLogger shouldReturn Logger("miscLogger")

    dao.readyPeers shouldReturn IO.pure(facilitators)

    dao
  }

  private def prepareMockedDao(facilitators: Map[Id, PeerData] = prepareFacilitators(1)): DAO = {
    import constellation._

    implicit val logger: io.chrisdavenport.log4cats.Logger[IO] = Slf4jLogger.getLogger
    implicit val contextShift = IO.contextShift(ConstellationExecutionContext.bounded)
    implicit val timer = IO.timer(ConstellationExecutionContext.unbounded)

    val dao: DAO = mock[DAO]
    val kp: KeyPair = makeKeyPair()
    dao.nodeConfig shouldReturn NodeConfig()
    dao.keyPair shouldReturn kp

    val f = File(s"tmp/${kp.getPublic.toId.medium}/db")
    f.createDirectoryIfNotExists()
    dao.dbPath shouldReturn f

    dao.id shouldReturn Fixtures.id

    val ss = new SOEService[IO]()
    dao.soeService shouldReturn ss

    val ns = new NotificationService[IO]()
    dao.notificationService shouldReturn ns

    val ms = {
      implicit val shadedDao = dao
      new MessageService[IO]()
    }
    dao.messageService shouldReturn ms

    val rl = RateLimiting[IO]()

    val txChain = TransactionChainService[IO]
    val ts = new TransactionService[IO](txChain, rl, dao)
    dao.transactionService shouldReturn ts

    val joiningPeerValidator: JoiningPeerValidator[IO] = new JoiningPeerValidator[IO]()
    dao.joiningPeerValidator shouldReturn joiningPeerValidator

    val cts = mock[ConcurrentTipService[IO]]

    val tm = mock[TrustManager[IO]]
    dao.trustManager shouldReturn tm

    val os = new ObservationService[IO](tm, dao)
    dao.observationService shouldReturn os

    dao.checkpointService shouldReturn mock[CheckpointService[IO]]

    val keyPair = KeyUtils.makeKeyPair()
    dao.keyPair shouldReturn keyPair

    dao.cluster shouldReturn mock[Cluster[IO]]
    dao.cluster.getNodeState shouldReturn IO.pure(NodeState.Ready)

    val metrics = new Metrics(1)(dao)
    dao.metrics shouldReturn metrics

    val ipManager = IPManager[IO]()
    val cluster = Cluster[IO](() => metrics, ipManager, joiningPeerValidator, dao)
    dao.cluster shouldReturn cluster
    dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready).unsafeRunSync

    dao.miscLogger shouldReturn Logger("miscLogger")

    dao.readyPeers shouldReturn IO.pure(facilitators)

    dao
  }

}
