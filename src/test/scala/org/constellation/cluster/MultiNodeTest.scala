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

import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.util.Success

class MultiNodeTest extends TestKit(ActorSystem("TestConstellationActorSystem")) with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "E2E Multiple Nodes" should "add peers and build blocks with transactions" ignore {

    val nodes = Seq.fill(3)(TestNode())

    for (node <- nodes) {
      assert(node.healthy)
    }

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

    nodes.foreach { node =>
      Future {
        val rpc1 = node.rpc
        val genResponse1 = rpc1.get("generateGenesisBlock")
        assert(genResponse1.get().status == StatusCodes.OK)
      }
    }

    Thread.sleep(2000)

    for (node <- nodes) {
      val rpc1 = node.rpc

      val chainStateNode1Response = rpc1.get("blocks")

      val response = chainStateNode1Response.get()
      println(s"ChainStateResponse $response")

      val chainNode1 = rpc1.read[Seq[Block]](response).get()

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

    var expectedTransactions = Set[Transaction]()

    nodes.foreach { node =>
      val rpc = node.rpc

      val transaction1 =
        Transaction.senderSign(Transaction(0L, node.id.id, nodes.head.id.id, 1L), node.keyPair.getPrivate)

      expectedTransactions = expectedTransactions.+(transaction1)

      rpc.post("transaction", transaction1)

      val transaction2 =
        Transaction.senderSign(Transaction(0L, node.id.id, nodes.last.id.id, 1L), node.keyPair.getPrivate)

      expectedTransactions = expectedTransactions.+(transaction2)

      rpc.post("transaction", transaction2)
    }

    assert(expectedTransactions.size == (nodes.length * 2))

    Thread.sleep(15000)

    nodes.foreach { n =>
      Future {
        val disableConsensusResponse1 = n.rpc.get("disableConsensus")
        assert(disableConsensusResponse1.get().status == StatusCodes.OK)
      }
    }

    Thread.sleep(1000)

    val blocks = nodes.map { n =>
      val finalChainStateNodeResponse = n.rpc.get("blocks")
      val finalChainNode = n.rpc.read[Seq[Block]](finalChainStateNodeResponse.get()).get()
      n.id -> finalChainNode
    }.toMap

    blocks.foreach(f => {
      val id = f._1
      val blockSize = f._2.size

      val transactions = f._2.flatMap(b => b.transactions).toSet

      println(s"for id = $id block size = $blockSize, transactionsSize = ${transactions.size}, transactions $transactions")

      assert(transactions.nonEmpty)

      assert(transactions.size == (nodes.size * 2))

      assert(transactions == expectedTransactions)
    })

    nodes.foreach{
      _.shutdown()
    }

    assert(true)
  }

}
