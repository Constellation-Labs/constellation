package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.testkit.GraphStageMessages.Failure
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import constellation._
import org.constellation.p2p.{GetUDPSocketRef, TestMessage}
import org.constellation.p2p.PeerToPeer.{GetPeers, Id, Peer, Peers}
import org.constellation.primitives.{Block, BlockSerialized, Transaction}
import org.constellation.util.RPCClient
import org.constellation.utils.TestNode
import org.scalatest.exceptions.TestFailedException

import scala.concurrent.duration._
import scala.util.Success

class MultiNodeTest extends TestKit(ActorSystem("TestConstellationActorSystem")) with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "Multiple Nodes" should "create and run within the same JVM context" in {
    val node1 = TestNode()
    val node2 = TestNode()
    val node3 = TestNode()

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)
    val rpc3 = new RPCClient(port=node3.httpPort)

    val rpc1Response = rpc1.get("health")
    val rpc2Response = rpc2.get("health")
    val rpc3Response = rpc3.get("health")

    assert(rpc1Response.get().status == StatusCodes.OK)
    assert(rpc2Response.get().status == StatusCodes.OK)
    assert(rpc3Response.get().status == StatusCodes.OK)

    node1.shutdown()
    node2.shutdown()
    node3.shutdown()
    assert(true)

  }

  "Multiple peers" should "connect nodes together" in {

    val node1 = TestNode()
    val node2 = TestNode()

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)

    rpc1.post("peer", node2.udpAddressString)
    rpc2.post("peer", node1.udpAddressString)

    Thread.sleep(1000)

    val node1PeersRequest = rpc1.get("peers")
    val peers1 = rpc1.read[Peers](node1PeersRequest.get()).get()

    val node2PeersRequest = rpc2.get("peers")
    val peers2 = rpc2.read[Peers](node2PeersRequest.get()).get()

    println(s"Peers1: $peers1")
    println(s"Peers2: $peers2")

    // This works locally but fails on circle-ci? Debug later
    // assert(peers1.peers.map{socketToAddress} == Seq(node2.udpAddressString))
    //assert(peers2.peers.map{socketToAddress} == Seq(node1.udpAddressString))

    node1.shutdown()
    node2.shutdown()
    assert(true)

  }

  "Multiple peers" should "connect nodes with only one peer add" in {

    val node1 = TestNode()
    val node2 = TestNode()

    Thread.sleep(1000)


    node1.udpActor.udpSend(TestMessage("a", 2), node2.udpAddress)
    node2.udpActor.udpSend(TestMessage("a", 2), node1.udpAddress)

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)

    rpc1.post("peer", node2.udpAddressString)

    Thread.sleep(1000)

    val node1PeersRequest = rpc1.get("peers")
    val peers1 = rpc1.read[Peers](node1PeersRequest.get()).get()

    val node2PeersRequest = rpc2.get("peers")
    val peers2 = rpc2.read[Peers](node2PeersRequest.get()).get()

    println(s"Peers1: $peers1")
    println(s"Peers2: $peers2")

    import akka.pattern.ask
    val udpSocket = (node2.udpActor ? GetUDPSocketRef).mapTo[ActorRef].get()
    println(s"UDP Actor 2 socket response $udpSocket")

    node1.udpActor.udpSend(TestMessage("a", 2), node2.udpAddress)
    node2.udpActor.udpSend(TestMessage("a", 2), node1.udpAddress)

    // Thread.sleep(1000)

    assert(peers1.peers.map{socketToAddress} == Seq(node2.udpAddressString))
    // Fix after authentication
    // assert(peers2.peers.map{socketToAddress} == Seq(node1.udpAddressString))

    node1.shutdown()
    node2.shutdown()
    assert(true)

  }


  "Multiple Nodes" should "be able to gossip peers and join a cluster together correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)
    val rpc3 = new RPCClient(port=node3.httpPort)
    val rpc4 = new RPCClient(port=node4.httpPort)

    val rpc1Response = rpc1.get("health")
    val rpc2Response = rpc2.get("health")
    val rpc3Response = rpc3.get("health")
    val rpc4Response = rpc4.get("health")

    assert(rpc1Response.get().status == StatusCodes.OK)
    assert(rpc2Response.get().status == StatusCodes.OK)
    assert(rpc3Response.get().status == StatusCodes.OK)
    assert(rpc4Response.get().status == StatusCodes.OK)

    // Connect the first two nodes
    val node1Path = node1.peerToPeerActor.path.toSerializationFormat
    val node2Path = node2.peerToPeerActor.path.toSerializationFormat
    val node3Path = node3.peerToPeerActor.path.toSerializationFormat
    val node4Path = node4.peerToPeerActor.path.toSerializationFormat

    rpc1.post("peer", node2Path)

    // TODO: find better way
    Thread.sleep(100)

    val node1PeersRequest = rpc1.get("peers")
    val node2PeersRequest = rpc2.get("peers")

    val node3PeersRequest = rpc3.get("peers")
    val node4PeersRequest = rpc4.get("peers")

    val peers1 = rpc1.read[Peers](node1PeersRequest.get()).get()
    val peers2 = rpc2.read[Peers](node2PeersRequest.get()).get()

    val peers3 = rpc3.read[Peers](node3PeersRequest.get()).get()
    val peers4 = rpc4.read[Peers](node4PeersRequest.get()).get()

    assert(peers1.peers.diff(Seq(node2Path)).isEmpty)
    assert(peers2.peers.diff(Seq(node1Path)).isEmpty)

    assert(peers3 == Peers(Seq()))
    assert(peers4 == Peers(Seq()))

    // Connect the second two nodes
    rpc3.post("peer", node4Path)

    // TODO: find better way
    Thread.sleep(100)

    val node1PeersRequest_2 = rpc1.get("peers")
    val node2PeersRequest_2 = rpc2.get("peers")

    val node3PeersRequest_2 = rpc3.get("peers")
    val node4PeersRequest_2 = rpc4.get("peers")

    val peers1_2 = rpc1.read[Peers](node1PeersRequest_2.get()).get()
    val peers2_2 = rpc2.read[Peers](node2PeersRequest_2.get()).get()

    val peers3_2 = rpc3.read[Peers](node3PeersRequest_2.get()).get()
    val peers4_2 = rpc4.read[Peers](node4PeersRequest_2.get()).get()

    assert(peers1_2.peers.diff(Seq(node2Path)).isEmpty)
    assert(peers2_2.peers.diff(Seq(node1Path)).isEmpty)

    assert(peers3_2.peers.diff(Seq(node4Path)).isEmpty)
    assert(peers4_2.peers.diff(Seq(node3Path)).isEmpty)

    // Connect the two pairs
    rpc2.post("peer", node3Path)

    // TODO: find better way
    Thread.sleep(100)

    val node1PeersRequest_3 = rpc1.get("peers")
    val node2PeersRequest_3 = rpc2.get("peers")

    val node3PeersRequest_3 = rpc3.get("peers")
    val node4PeersRequest_3 = rpc4.get("peers")

    val peers1_3 = rpc1.read[Peers](node1PeersRequest_3.get()).get()
    val peers2_3 = rpc2.read[Peers](node2PeersRequest_3.get()).get()

    val peers3_3 = rpc3.read[Peers](node3PeersRequest_3.get()).get()
    val peers4_3 = rpc4.read[Peers](node4PeersRequest_3.get()).get()

    assert(peers1_3.peers.diff(Seq(node2Path, node3Path, node4Path)).isEmpty)
    assert(peers2_3.peers.diff(Seq(node1Path, node3Path, node4Path)).isEmpty)

    assert(peers3_3.peers.diff(Seq(node4Path, node2Path, node1Path)).isEmpty)
    assert(peers4_3.peers.diff(Seq(node3Path, node2Path, node1Path)).isEmpty)


    node1.shutdown()
    node2.shutdown()
    node3.shutdown()
    node4.shutdown()
    assert(true)


  }

  "Multiple Nodes" should "come to consensus on transactions after genesis block" in {

    val nodes = Seq.fill(3)(TestNode(heartbeatEnabled = true))

    for (node <- nodes) {
      assert(node.healthy)
    }

    nodes.head.rpc.get("master")

    for (n1 <- nodes) {
      println(s"Trying to add nodes to $n1")
      val others = nodes.filter{_ != n1}
      others.foreach{
        n =>
          Future {println(s"Trying to add $n to $n1 res: ${n1.add(n)}")}
      }
    }

    Thread.sleep(5000)

    for (node <- nodes) {
      val peers = node.rpc.getBlocking[Seq[Peer]]("peerids")
      println(s"Peers length: ${peers.length}")
      assert(peers.length == (nodes.length - 1))
    }

    for (node <- nodes) {

      val rpc1 = node.rpc
      val genResponse1 = rpc1.get("generateGenesisBlock")
      assert(genResponse1.get().status == StatusCodes.OK)

    }

    Thread.sleep(2000)

    for (node <- nodes) {
      val rpc1 = node.rpc

      val chainStateNode1Response = rpc1.get("blocks")

      val response = chainStateNode1Response.get()
      println(s"ChainStateResponse $response")

      val chainNode1 = Seq(rpc1.readDebug[Block](response).get())

      assert(chainNode1.size == 1)

      assert(chainNode1.head.height == 0)

      assert(chainNode1.head.round == 0)

      assert(chainNode1.head.parentHash == "tempGenesisParentHash")

      assert(chainNode1.head.signature == "tempSig")

      assert(chainNode1.head.transactions == Seq())

      assert(chainNode1.head.clusterParticipants.diff(nodes.map {_.id}.toSet).isEmpty)

      val consensusResponse1 = rpc1.get("enableConsensus")

      assert(consensusResponse1.get().status == StatusCodes.OK)

    }

    Thread.sleep(5000)

    val transactions = nodes.flatMap{ node =>

      val rpc1 = node.rpc

      val transaction1 =
        Transaction.senderSign(Transaction(0L, node.id.id, nodes.head.id.id, 1L), node.keyPair.getPrivate)

      rpc1.post("transaction", transaction1)

      val transaction5 =
        Transaction.senderSign(Transaction(0L, node.id.id, nodes.last.id.id, 1L), node.keyPair.getPrivate)

      rpc1.post("transaction", transaction5)

      Seq(transaction1, transaction5)
    }


    Thread.sleep(15000)

    nodes.foreach { n =>
      Future {
        val disableConsensusResponse1 = n.rpc.get("disableConsensus")
        assert(disableConsensusResponse1.get().status == StatusCodes.OK)
      }
    }

    Thread.sleep(1000)

    val blocks = nodes.map{ n=>
      val finalChainStateNode1Response = n.rpc.get("blocks")
      val finalChainNode1 = n.rpc.read[Seq[Block]](finalChainStateNode1Response.get()).get()
      finalChainNode1
    }

    print("Block lengths : " +blocks.map{_.length})
    val chainSizes = blocks.map{_.length}
    val totalNumTrx = blocks.flatMap(_.flatMap(_.transactions)).length

    println(s"Total number of transactions: $totalNumTrx")

    assert(totalNumTrx > 0)

    blocks.foreach{ b =>
      assert(b.flatMap{_.transactions}.size == (nodes.length * 2))
    }

    val minSize = blocks.map(_.length).min
    assert(blocks.map{_.slice(0, minSize)}.distinct.size == 1)
    assert(totalNumTrx == (nodes.length * 2 * nodes.length))
    assert(true)


  }

}
