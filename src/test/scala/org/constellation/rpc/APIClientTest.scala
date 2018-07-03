package org.constellation.rpc

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor
import constellation._
import org.constellation.Fixtures
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.Schema.{Id, Peers}
import org.constellation.primitives.Transaction
import org.constellation.util.APIClient
import org.constellation.util.TestNode

class APIClientTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  /*

    val kp1 = r1.getBlocking[KeyPair]("makeKeyPair")
    val kp2 = r1.getBlocking[KeyPair]("makeKeyPair")
    val wallet = r1.getBlocking[Seq[KeyPair]]("wallet")

    assert(!kp1.dataEqual(kp2))
    assert(wallet.head.dataEqual(kp1))
    assert(wallet.last.dataEqual(kp2))

   */

  "GET to /blocks" should "get the current local node chain" in {
    val appNode = TestNode()
    val rpc = new APIClient(port=appNode.httpPort)

    val response = rpc.get("blocks")

    val localChain = rpc.read[Chain](response.get()).get()

    assert(localChain == Chain())
  }

  import Fixtures._

  "GET to /peers" should "get the correct connected peers" in {

    val node1 = TestNode()

    val node1Path = node1.udpAddress

    val expectedPeers = Seq(node1Path)

    val node2 = TestNode(expectedPeers)

    val rpc = new APIClient(port=node2.httpPort)

    Thread.sleep(2000)

    val response = rpc.get("peers")

    val actualPeers = rpc.read[Peers](response.get()).get()

    println(actualPeers)

    assert(actualPeers.peers.contains(node1Path))
  }

  "GET to /id" should "get the current nodes public key id" in {
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(Seq(), keyPair)
    val rpc = new APIClient(port=appNode.httpPort)

    val response = rpc.get("id")

    val id = rpc.read[Id](response.get()).get()

    assert(Id(keyPair.getPublic.encoded) == id)
  }

  "GET to /balance" should "get the correct current balance for the provided pubKey" in {
    /* TODO
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(None, keyPair)
    val rpc = new RPCClient(port=appNode.httpPort)

    val response = rpc.get("id")

    val id = rpc.read[Id](response.get()).get()

    assert(Id(keyPair.getPublic) == id)
    */
  }

  "POST to /transaction" should "send a transaction and receive it back" in {
    val appNode = TestNode()
    val rpc = new APIClient(port=appNode.httpPort)

    val transaction = Fixtures.tx
    val response = rpc.post("transaction", transaction)
    val transactionResponse = rpc.read[Transaction](response.get()).get()

    assert(transaction == transactionResponse)
  }

  "POST to /peer" should "add the peer correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()

    val node1Path = node1.udpAddressString
    val node2Path = node2.udpAddressString

    val rpc1 = new APIClient(port=node1.httpPort)
    val rpc2 = new APIClient(port=node2.httpPort)

    val addPeerResponse = rpc2.post("peer", node1Path)

    assert(addPeerResponse.get().status == StatusCodes.OK)

    // TODO: bug here with lookup of peer, execution context issue, timing?

    val peersResponse1 = rpc1.get("peers")
    val peersResponse2 = rpc2.get("peers")

    val peers1 = rpc1.read[Peers](peersResponse1.get()).get()
    val peers2 = rpc1.read[Peers](peersResponse2.get()).get()

    // Re-enable after we allow peer adding from authenticated peer.
   // assert(peers1 == Peers(Seq(node2Path)))
    assert(peers2 == Peers(Seq(node1Path)))

  }

  override def afterAll() {
    system.terminate()
  }

}