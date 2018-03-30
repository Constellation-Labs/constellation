package org.constellation.cluster

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContextExecutor
import constellation._
import org.constellation.Fixtures
import org.constellation.consensus.Consensus.MemPoolUpdated
import org.constellation.p2p.PeerToPeer.{Id, Peers}
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.{BlockSerialized, Transaction}
import org.constellation.utils.{RPCClient, TestNode}
import org.constellation.wallet.KeyUtils
import scala.concurrent.duration._

import scala.concurrent.duration.Duration

class MultiNodeTest extends TestKit(ActorSystem("TestConstellationActorSystem")) with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

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

  }

  "Multiple Nodes" should "come to consensus on transactions after genesis block" in {

    // makeKeyPair
    // Transaction()
    // senderSign
    // counterPartySign

    // Create the nodes
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

    // verify that they are up
    assert(rpc1Response.get().status == StatusCodes.OK)
    assert(rpc2Response.get().status == StatusCodes.OK)
    assert(rpc3Response.get().status == StatusCodes.OK)
    assert(rpc4Response.get().status == StatusCodes.OK)

    // Connect them together
    val node1Path = node1.peerToPeerActor.path.toSerializationFormat
    val node2Path = node2.peerToPeerActor.path.toSerializationFormat
    val node3Path = node3.peerToPeerActor.path.toSerializationFormat
    val node4Path = node4.peerToPeerActor.path.toSerializationFormat

    rpc1.post("peer", node2Path)

    // TODO: find better way
    Thread.sleep(100)

    rpc2.post("peer", node3Path)

    Thread.sleep(100)

    rpc3.post("peer", node4Path)

    Thread.sleep(100)

    val node1PeersRequest = rpc1.get("peers")
    val node2PeersRequest = rpc2.get("peers")
    val node3PeersRequest = rpc3.get("peers")
    val node4PeersRequest = rpc4.get("peers")

    val peers1 = rpc1.read[Peers](node1PeersRequest.get()).get()
    val peers2 = rpc2.read[Peers](node2PeersRequest.get()).get()
    val peers3 = rpc3.read[Peers](node3PeersRequest.get()).get()
    val peers4 = rpc4.read[Peers](node4PeersRequest.get()).get()

    // Verify that they are connected
    assert(peers1.peers.diff(Seq(node2Path, node3Path, node4Path)).isEmpty)
    assert(peers2.peers.diff(Seq(node1Path, node3Path, node4Path)).isEmpty)
    assert(peers3.peers.diff(Seq(node1Path, node4Path, node2Path)).isEmpty)
    assert(peers4.peers.diff(Seq(node1Path, node3Path, node2Path)).isEmpty)

    // generate genesis block
    val genResponse1 = rpc1.get("generateGenesisBlock")

    assert(genResponse1.get().status == StatusCodes.OK)

    val genResponse2 = rpc2.get("generateGenesisBlock")

    assert(genResponse2.get().status == StatusCodes.OK)

    val genResponse3 = rpc3.get("generateGenesisBlock")

    assert(genResponse3.get().status == StatusCodes.OK)

    val genResponse4 = rpc4.get("generateGenesisBlock")

    assert(genResponse4.get().status == StatusCodes.OK)

    Thread.sleep(100)

    // TODO: extract below to func
    // verify chain node 1
    val chainStateNode1Response = rpc1.get("blocks")

    val chainNode1 = rpc1.read[Seq[BlockSerialized]](chainStateNode1Response.get()).get()

    assert(chainNode1.size == 1)

    assert(chainNode1.head.height == 0)

    assert(chainNode1.head.round == 0)

    assert(chainNode1.head.parentHash == "tempGenesisParentHash")

    assert(chainNode1.head.signature == "tempSig")

    assert(chainNode1.head.transactions == Seq())

    assert(chainNode1.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

    // verify chain node 2
    val chainStateNode2Response = rpc2.get("blocks")

    val chainNode2 = rpc2.read[Seq[BlockSerialized]](chainStateNode2Response.get()).get()

    assert(chainNode2.size == 1)

    assert(chainNode2.head.height == 0)

    assert(chainNode2.head.round == 0)

    assert(chainNode2.head.parentHash == "tempGenesisParentHash")

    assert(chainNode2.head.signature == "tempSig")

    assert(chainNode2.head.transactions == Seq())

    assert(chainNode2.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

    // verify chain node 3
    val chainStateNode3Response = rpc3.get("blocks")

    val chainNode3 = rpc3.read[Seq[BlockSerialized]](chainStateNode3Response.get()).get()

    assert(chainNode3.size == 1)

    assert(chainNode3.head.height == 0)

    assert(chainNode3.head.round == 0)

    assert(chainNode3.head.parentHash == "tempGenesisParentHash")

    assert(chainNode3.head.signature == "tempSig")

    assert(chainNode3.head.transactions == Seq())

    assert(chainNode3.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

    // verify chain node 4
    val chainStateNode4Response = rpc4.get("blocks")

    val chainNode4= rpc4.read[Seq[BlockSerialized]](chainStateNode4Response.get()).get()

    assert(chainNode4.size == 1)

    assert(chainNode4.head.height == 0)

    assert(chainNode4.head.round == 0)

    assert(chainNode4.head.parentHash == "tempGenesisParentHash")

    assert(chainNode4.head.signature == "tempSig")

    assert(chainNode4.head.transactions == Seq())

    assert(chainNode4.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

    // Enable consensus on the nodes
    val consensusResponse1 = rpc1.get("enableConsensus")

    assert(consensusResponse1.get().status == StatusCodes.OK)

    val consensusResponse2 = rpc2.get("enableConsensus")

    assert(consensusResponse2.get().status == StatusCodes.OK)

    val consensusResponse3 = rpc3.get("enableConsensus")

    assert(consensusResponse3.get().status == StatusCodes.OK)

    val consensusResponse4 = rpc4.get("enableConsensus")

    assert(consensusResponse4.get().status == StatusCodes.OK)

    val probe = TestProbe()

    probe watch node2.consensusActor

    probe.expectMsg(120 seconds, MemPoolUpdated)

  }

}
