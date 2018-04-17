package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.testkit.GraphStageMessages.Failure
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import constellation._
import org.constellation.p2p.PeerToPeer.{GetPeers, Peers}
import org.constellation.primitives.{BlockSerialized, Transaction}
import org.constellation.utils.{RPCClient, TestNode}
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
    // Create the nodes
    val node1 = TestNode()
    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()

    val node1Path = node1.udpAddressString
    val node2Path = node2.udpAddressString
    val node3Path = node3.udpAddressString
    val node4Path = node4.udpAddressString

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)
    val rpc3 = new RPCClient(port=node3.httpPort)
    val rpc4 = new RPCClient(port=node4.httpPort)

    val node1PublicKey = node1.keyPair.getPublic
    val node2PublicKey = node2.keyPair.getPublic
    val node3PublicKey = node3.keyPair.getPublic
    val node4PublicKey = node4.keyPair.getPublic

    val node1Future = Future {
      val rpc1Response = rpc1.get("health")

      assert(rpc1Response.get().status == StatusCodes.OK)

      rpc1.post("peer", node2Path)
      rpc1.post("peer", node3Path)
      rpc1.post("peer", node4Path)

      // TODO: find better way
      Thread.sleep(1000)

      val node1PeersRequest = rpc1.get("peers")
      val peers1 = rpc1.read[Peers](node1PeersRequest.get()).get()

      assert(Seq(node2Path, node3Path, node4Path).diff(peers1.peers.map{socketToAddress}).isEmpty)

      val genResponse1 = rpc1.get("generateGenesisBlock")
      assert(genResponse1.get().status == StatusCodes.OK)
      val chainStateNode1Response = rpc1.get("blocks")

      val chainNode1 = rpc1.read[Seq[BlockSerialized]](chainStateNode1Response.get()).get()

      assert(chainNode1.size == 1)

      assert(chainNode1.head.height == 0)

      assert(chainNode1.head.round == 0)

      assert(chainNode1.head.parentHash == "tempGenesisParentHash")

      assert(chainNode1.head.signature == "tempSig")

      assert(chainNode1.head.transactions == Seq())

      assert(chainNode1.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

      val consensusResponse1 = rpc1.get("enableConsensus")

      assert(consensusResponse1.get().status == StatusCodes.OK)

      val transaction1 =
        Transaction.senderSign(Transaction(0L, node1PublicKey, node2PublicKey, 1L), node1.keyPair.getPrivate)

      rpc1.post("transaction", transaction1)

      val transaction5 =
        Transaction.senderSign(Transaction(4L, node3PublicKey, node4PublicKey, 5L), node3.keyPair.getPrivate)

      rpc1.post("transaction", transaction5)

      val disableConsensusResponse1 = rpc1.get("disableConsensus")

      assert(disableConsensusResponse1.get().status == StatusCodes.OK)

      true
    }

    val node2Future = Future {
      val rpc2Response = rpc2.get("health")
      assert(rpc2Response.get().status == StatusCodes.OK)

      rpc2.post("peer", node3Path)
      rpc2.post("peer", node2Path)
      rpc2.post("peer", node4Path)

      Thread.sleep(1000)

      val node2PeersRequest = rpc2.get("peers")
      val peers2 = rpc2.read[Peers](node2PeersRequest.get()).get()

      assert(Seq(node1Path, node3Path, node4Path).diff(peers2.peers.map{socketToAddress}).isEmpty)

      val genResponse2 = rpc2.get("generateGenesisBlock")
      assert(genResponse2.get().status == StatusCodes.OK)

      val chainStateNode2Response = rpc2.get("blocks")

      val chainNode2 = rpc2.read[Seq[BlockSerialized]](chainStateNode2Response.get()).get()

      assert(chainNode2.size == 1)

      assert(chainNode2.head.height == 0)

      assert(chainNode2.head.round == 0)

      assert(chainNode2.head.parentHash == "tempGenesisParentHash")

      assert(chainNode2.head.signature == "tempSig")

      assert(chainNode2.head.transactions == Seq())

      assert(chainNode2.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

      val consensusResponse2 = rpc2.get("enableConsensus")

      assert(consensusResponse2.get().status == StatusCodes.OK)

      val transaction3 =
        Transaction.senderSign(Transaction(2L, node1PublicKey, node4PublicKey, 33L), node1.keyPair.getPrivate)

      rpc2.post("transaction", transaction3)

      val transaction6 =
        Transaction.senderSign(Transaction(5L, node3PublicKey, node2PublicKey, 1L), node3.keyPair.getPrivate)

      rpc2.post("transaction", transaction6)

      val disableConsensusResponse2 = rpc2.get("disableConsensus")

      assert(disableConsensusResponse2.get().status == StatusCodes.OK)

      true
    }

    val node3Future = Future {
      val rpc3Response = rpc3.get("health")
      assert(rpc3Response.get().status == StatusCodes.OK)

      rpc3.post("peer", node4Path)
      rpc3.post("peer", node1Path)
      rpc3.post("peer", node2Path)

      Thread.sleep(1000)

      val node3PeersRequest = rpc3.get("peers")
      val peers3 = rpc3.read[Peers](node3PeersRequest.get()).get()

      assert(Seq(node1Path, node4Path, node2Path).diff(peers3.peers.map{socketToAddress}).isEmpty)

      val genResponse3 = rpc3.get("generateGenesisBlock")
      assert(genResponse3.get().status == StatusCodes.OK)

      val chainStateNode3Response = rpc3.get("blocks")

      val chainNode3 = rpc3.read[Seq[BlockSerialized]](chainStateNode3Response.get()).get()

      assert(chainNode3.size == 1)

      assert(chainNode3.head.height == 0)

      assert(chainNode3.head.round == 0)

      assert(chainNode3.head.parentHash == "tempGenesisParentHash")

      assert(chainNode3.head.signature == "tempSig")

      assert(chainNode3.head.transactions == Seq())

      assert(chainNode3.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

      val consensusResponse3 = rpc3.get("enableConsensus")

      assert(consensusResponse3.get().status == StatusCodes.OK)

      val transaction2 =
        Transaction.senderSign(Transaction(1L, node2PublicKey, node3PublicKey, 20L), node2.keyPair.getPrivate)

      rpc3.post("transaction", transaction2)

      val transaction4 =
        Transaction.senderSign(Transaction(3L, node3PublicKey, node4PublicKey, 10L), node3.keyPair.getPrivate)

      rpc3.post("transaction", transaction4)

      val disableConsensusResponse3 = rpc3.get("disableConsensus")

      assert(disableConsensusResponse3.get().status == StatusCodes.OK)

      true
    }

    val node4Future = Future {
      val rpc4Response = rpc4.get("health")
      assert(rpc4Response.get().status == StatusCodes.OK)

      rpc4.post("peer", node1Path)
      rpc4.post("peer", node2Path)
      rpc4.post("peer", node3Path)

      Thread.sleep(1000)

      val node4PeersRequest = rpc4.get("peers")
      val peers4 = rpc4.read[Peers](node4PeersRequest.get()).get()

      assert(Seq(node1Path, node3Path, node2Path).diff(peers4.peers.map{socketToAddress}).isEmpty)

      val genResponse4 = rpc4.get("generateGenesisBlock")
      assert(genResponse4.get().status == StatusCodes.OK)

      val chainStateNode4Response = rpc4.get("blocks")

      val chainNode4= rpc4.read[Seq[BlockSerialized]](chainStateNode4Response.get()).get()

      assert(chainNode4.size == 1)

      assert(chainNode4.head.height == 0)

      assert(chainNode4.head.round == 0)

      assert(chainNode4.head.parentHash == "tempGenesisParentHash")

      assert(chainNode4.head.signature == "tempSig")

      assert(chainNode4.head.transactions == Seq())

      assert(chainNode4.head.clusterParticipants.diff(Set(node1Path, node2Path, node3Path, node4Path)).isEmpty)

      val consensusResponse4 = rpc4.get("enableConsensus")

      assert(consensusResponse4.get().status == StatusCodes.OK)

      val disableConsensusResponse4 = rpc4.get("disableConsensus")

      assert(disableConsensusResponse4.get().status == StatusCodes.OK)

      true
    }

    node1Future.map(f => {
      assert(f == true)
    })

    node2Future.map(f => {
      assert(f == true)
    })

    node3Future.map(f => {
      assert(f == true)
    })

    node4Future.map(f => {
      assert(f == true)
    })

    Future.sequence(Seq(node1Future, node2Future, node3Future, node4Future)).map(f => {
      Thread.sleep(100000)

      val finalChainStateNode1Response = rpc1.get("blocks")

      val finalChainNode1 = rpc1.read[Seq[BlockSerialized]](finalChainStateNode1Response.get()).get()

      val finalChainStateNode2Response = rpc2.get("blocks")

      val finalChainNode2 = rpc2.read[Seq[BlockSerialized]](finalChainStateNode2Response.get()).get()

      val finalChainStateNode3Response = rpc3.get("blocks")

      val finalChainNode3 = rpc3.read[Seq[BlockSerialized]](finalChainStateNode3Response.get()).get()

      val finalChainStateNode4Response = rpc4.get("blocks")

      val finalChainNode4 = rpc4.read[Seq[BlockSerialized]](finalChainStateNode4Response.get()).get()

      val shortestChainLength = List(finalChainNode1.size, finalChainNode2.size, finalChainNode3.size, finalChainNode4.size).min

      val chain1 = finalChainNode1.take(shortestChainLength)
      val chain2 = finalChainNode2.take(shortestChainLength)
      val chain3 = finalChainNode3.take(shortestChainLength)
      val chain4 = finalChainNode4.take(shortestChainLength)

      var chain1ContainsTransactions = false
      var chain2ContainsTransactions = false
      var chain3ContainsTransactions = false
      var chain4ContainsTransactions = false

      var chain1TransactionCount = 0
      var chain2TransactionCount = 0
      var chain3TransactionCount = 0
      var chain4TransactionCount = 0

      chain1.foreach(b => {
        if (b.transactions.nonEmpty) {
          chain1ContainsTransactions = true
          chain1TransactionCount += b.transactions.size
        }
      })

      chain2.foreach(b => {
        if (b.transactions.nonEmpty) {
          chain2ContainsTransactions = true
          chain2TransactionCount += b.transactions.size
        }
      })

      chain3.foreach(b => {
        if (b.transactions.nonEmpty) {
          chain3ContainsTransactions = true
          chain3TransactionCount += b.transactions.size
        }
      })

      chain4.foreach(b => {
        if (b.transactions.nonEmpty) {
          chain4ContainsTransactions = true
          chain4TransactionCount += b.transactions.size
        }
      })

      // Make sure that from the subset of blocks that we have it contains transactions
      assert(chain1ContainsTransactions)
      assert(chain2ContainsTransactions)
      assert(chain3ContainsTransactions)
      assert(chain4ContainsTransactions)

      assert(chain1TransactionCount == 6)
      assert(chain2TransactionCount == 6)
      assert(chain3TransactionCount == 6)
      assert(chain4TransactionCount == 6)

      println(s"CHAIN1: $chain1")
      println(s"CHAIN2: $chain2")


      assert(chain1.zip(chain2).forall{
        case (b1, b2) =>
          if (b1 != b2) {
            println("BLOCK NOT EQUAL 1: " + b1)
            println("BLOCK NOT EQUAL 2: " + b2)
          } else {
            println("BLOCK EQUAL")
          }
          b1 == b2
      })

      assert(chain2.zip(chain3).forall{
        case (b1, b2) =>
          if (b1 != b2) {
            println("BLOCK NOT EQUAL 1: " + b1)
            println("BLOCK NOT EQUAL 2: " + b2)
          } else {
            println("BLOCK EQUAL")
          }
          b1 == b2
      })


      assert(chain3.zip(chain4).forall{
        case (b1, b2) =>
          if (b1 != b2) {
            println("BLOCK NOT EQUAL 1: " + b1)
            println("BLOCK NOT EQUAL 2: " + b2)
          } else {
            println("BLOCK EQUAL")
          }
          b1 == b2
      })

      assert(f == true)
    })

  }

}
