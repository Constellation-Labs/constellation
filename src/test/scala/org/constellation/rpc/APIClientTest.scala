package org.constellation.rpc

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.{Id, Peers}
import org.constellation.util.{APIClient, TestNode}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor

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

  "GET to /peers" should "get the correct connected peers" in {

    val node1 = TestNode()

    val node1Path = node1.udpAddress

    val expectedPeers = Seq(node1Path)

    val node2 = TestNode(expectedPeers)

    val rpc = new APIClient(port=node2.httpPort)

    Thread.sleep(2000)

    val actualPeers = rpc.getBlocking[Peers]("peers")

    println(actualPeers)

    assert(actualPeers.peers.contains(node1Path))
  }

  "GET to /id" should "get the current nodes public key id" in {
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(Seq(), keyPair)
    val rpc = new APIClient(port=appNode.httpPort)

    val id = rpc.getBlocking[Id]("id")

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

  "POST to /peer" should "add the peer correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()

    val node1Path = node1.udpAddressString
    val node2Path = node2.udpAddressString

    val rpc1 = new APIClient(port=node1.httpPort)
    val rpc2 = new APIClient(port=node2.httpPort)

    val addPeerResponse = rpc2.postSync("peer", node1Path)

    assert(addPeerResponse.isSuccess)

    // TODO: bug here with lookup of peer, execution context issue, timing?

    val peers1 = rpc1.getBlocking[Peers]("peers")
    val peers2 = rpc2.getBlocking[Peers]("peers")

    // Re-enable after we allow peer adding from authenticated peer.
   // assert(peers1 == Peers(Seq(node2Path)))
    assert(peers2 == Peers(Seq(node1Path)))

  }

  override def afterAll() {
    system.terminate()
  }

}