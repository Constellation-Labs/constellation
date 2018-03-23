package org.constellation.rpc

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor
import constellation._
import org.constellation.Fixtures
import org.constellation.p2p.PeerToPeer.{Id, Peers}
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.Transaction
import org.constellation.utils.{RPCClient, TestNode}
import org.constellation.wallet.KeyUtils

class RPCClientTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  "GET to /blocks" should "get the current local node chain" in {
    val appNode = TestNode()
    val rpc = new RPCClient(port=appNode.httpPort)

    val response = rpc.get("blocks")

    val localChain = rpc.read[Chain](response.get()).get()

    assert(localChain == Chain())
  }

  "GET to /peers" should "get the correct connected peers" in {
    val node1 = TestNode()

    val node1Path = node1.peerToPeerActor.path.toSerializationFormat

    val expectedPeers = Some(Seq(node1Path))

    val node2 = TestNode(expectedPeers)

    val rpc = new RPCClient(port=node2.httpPort)

    val response = rpc.get("peers")

    val actualPeers = rpc.read[Peers](response.get()).get()

    assert(Peers(expectedPeers.get) == actualPeers)
  }

  "GET to /id" should "get the current nodes public key id" in {
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(None, keyPair)
    val rpc = new RPCClient(port=appNode.httpPort)

    val response = rpc.get("id")

    val id = rpc.read[Id](response.get()).get()

    assert(Id(keyPair.getPublic) == id)
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
    val rpc = new RPCClient(port=appNode.httpPort)

    val transaction = Fixtures.tx
    val response = rpc.post("transaction", transaction)
    val transactionResponse = rpc.read[Transaction](response.get()).get()

    assert(transaction == transactionResponse)
  }

  "POST to /peer" should "add the peer correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()

    val node1Path = node1.peerToPeerActor.path.toSerializationFormat
    val node2Path = node2.peerToPeerActor.path.toSerializationFormat

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)

    val addPeerResponse = rpc2.post("peer", node1Path)

    assert(addPeerResponse.get().status == StatusCodes.Created)

    // TODO: bug here with lookup of peer, execution context issue, timing?

    val peersResponse1 = rpc1.get("peers")
    val peersResponse2 = rpc2.get("peers")

    val peers1 = rpc1.read[Peers](peersResponse1.get()).get()
    val peers2 = rpc1.read[Peers](peersResponse2.get()).get()

    assert(peers1 == Peers(Seq(node2Path)))
    assert(peers2 == Peers(Seq(node1Path)))

  }

  override def afterAll() {
    system.terminate()
  }

}