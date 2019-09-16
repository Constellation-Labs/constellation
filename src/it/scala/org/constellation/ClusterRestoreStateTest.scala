package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import com.typesafe.scalalogging.Logger
import org.constellation.util.APIClient
import org.constellation.util.Simulation._
import org.scalatest.{BeforeAndAfterAll, CancelAfterFailure, FlatSpecLike}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future}

class ClusterRestoreStateTest
    extends TestKit(ActorSystem("ClusterRestoreStateTest"))
    with FlatSpecLike
    with BeforeAndAfterAll
    with CancelAfterFailure {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()

  implicit val backend: SttpBackend[Future, Nothing] =
    PrometheusBackend[Future, Nothing](OkHttpFutureBackend()(ConstellationExecutionContext.unbounded))

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()

  val apis: Seq[APIClient] = ComputeTestUtil.createApisFromIpFile(ignoreIPs)

  val peerRequests: Seq[PeerMetadata] = apis.map { a =>
    PeerMetadata(
      a.hostName,
      a.peerHTTPPort,
      a.id,
      resourceInfo = ResourceInfo(diskUsableBytes = 1073741824)
    )
  }

  "Cluster integration" should "should restore state" in {
    assert(checkHealthy(apis))

    setIdLocal(apis)
    logger.info("Local id set")

    disableRandomTransactions(apis)
    disableCheckpointFormation(apis)
    logger.info("RandomTransaction and CheckpointFormation disabled")

    addPeersFromRequest(apis, peerRequests)
    logger.info("Peers added")

    restoreState(apis)
    logger.info("State restored")

    setReady(apis)
    logger.info("Node ready")

    enableRandomTransactions(apis)
    enableCheckpointFormation(apis)
    logger.info("RandomTransaction and CheckpointFormation enabled")
  }
}
