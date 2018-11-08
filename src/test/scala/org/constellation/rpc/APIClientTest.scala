package org.constellation.rpc

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.Id
import org.constellation.util.{APIClient, TestNode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor

class APIClientTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterEach(): Unit = {
    TestNode.clearNodes()
  }


  "GET to /peers" should "get the correct connected peers" in {

    val node1 = TestNode()

    val node1Path = node1.udpAddress

    val expectedPeers = Seq(node1Path)

    val node2 = TestNode() //expectedPeers)

    // TODO: Change to new peer add function
/*    val rpc = APIClient(port = node2.httpPort)

    Thread.sleep(2000)

    val actualPeers = rpc.getBlocking[Peers]("peers")

    println(actualPeers)

    assert(actualPeers.peers.contains(node1Path))*/
  }

  "GET to /id" should "get the current nodes public key id" in {
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(Seq(), keyPair)
    val rpc = APIClient(port=appNode.httpPort)
    val id = rpc.getBlocking[Id]("id")

    assert(Id(keyPair.getPublic.encoded) == id)
  }


  "POST to /peer" should "add the peer correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()

    val node1Path = node1.udpAddressString
    val node2Path = node2.udpAddressString

    val rpc1 = APIClient(port=node1.httpPort)
    val rpc2 = APIClient(port=node2.httpPort)

    val addPeerResponse = rpc2.postSync("peer", node1Path)
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