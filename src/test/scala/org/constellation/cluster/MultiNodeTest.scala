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

  "E2E Multiple Nodes" should "add peers and build blocks with transactions" in {

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
