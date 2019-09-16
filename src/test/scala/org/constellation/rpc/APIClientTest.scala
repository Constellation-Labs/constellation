package org.constellation.rpc

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import constellation._
import org.constellation.ConstellationExecutionContext
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.Id
import org.constellation.util.{APIClient, TestNode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.{ExecutionContextExecutor, Future}

class APIClientTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()

  implicit val backend: SttpBackend[Future, Nothing] =
    PrometheusBackend[Future, Nothing](OkHttpFutureBackend()(ConstellationExecutionContext.unbounded))

  override def afterEach(): Unit =
    TestNode.clearNodes()

  "GET to /id" should "get the current nodes public key id" in {
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(Seq(), keyPair)
    val rpc = APIClient(port = appNode.nodeConfig.httpPort)
    val id = rpc.getBlocking[Id]("id")

    assert(keyPair.getPublic.toId == id)
  }

  "POST to /peer" should "add the peer correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()

    val rpc1 = APIClient(node1.nodeConfig.hostName, port = node1.nodeConfig.httpPort)
    val rpc2 = APIClient(node2.nodeConfig.hostName, port = node2.nodeConfig.httpPort)

    val addPeerResponse = rpc2.postSync("peer/add", node1.dao.peerHostPort)

    assert(addPeerResponse.isSuccess)
    // TODO: Change this to AddPeerFromLocal request on REST
    /*
    assert(addPeerResponse.isSuccess)

    // TODO: bug here with lookup of peer, execution context issue, timing?

    val peers1 = rpc1.getBlocking[Map[Id, PeerData]]("peers")
    val peers2 = rpc2.getBlocking[Map[Id, PeerData]]("peers")

    assert(peers1.contains(node2.data.id))
    assert(peers2.contains(node1.data.id))
     */

    // Re-enable after we allow peer adding from authenticated peer.
    // assert(peers1 == Peers(Seq(node2Path)))
    //  assert(peers2 == Peers(Seq(node1Path)))

  }

  override def afterAll() {
    system.terminate()
  }

}
