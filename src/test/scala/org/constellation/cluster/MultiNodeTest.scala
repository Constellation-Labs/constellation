package org.constellation.cluster

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.testkit.GraphStageMessages.Failure
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import constellation._
import org.constellation.ConstellationNode
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

  "E2E Multiple Nodes" should "add peers and build blocks with transactions" in {

    val numberOfNodes = 3

    val nodes = Seq.fill(numberOfNodes)(TestNode())

    val futures = nodes.map(node => {
      Future {

        assert(node.healthy)

        for (n1 <- nodes) {
          println(s"Trying to add nodes to $n1")

          val others = nodes.filter{_ != n1}

          others.foreach { n => {
            n1.add(n)
            println(s"Trying to add $n to $n1")
            }
          }

        }

        val peers = node.rpc.getBlocking[Seq[Peer]]("peerids")
        println(s"Peers length: ${peers.length}")
        assert(peers.length == (nodes.length - 1))

        val rpc1 = node.rpc
        val genResponse1 = rpc1.get("generateGenesisBlock")
        assert(genResponse1.get().status == StatusCodes.OK)

        Thread.sleep(3000)

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

        Thread.sleep(3000)

        val rpc = node.rpc

        val transaction1 =
          Transaction.senderSign(Transaction(0L, node.id.id, nodes.head.id.id, 1L), node.keyPair.getPrivate)

        rpc.post("transaction", transaction1)

        val transaction2 =
          Transaction.senderSign(Transaction(0L, node.id.id, nodes.last.id.id, 1L), node.keyPair.getPrivate)

        rpc.post("transaction", transaction2)

        Thread.sleep(6000)

        true
      }
    })

    val sequence = Future.sequence(futures)

    Await.result(sequence, 45 seconds)

    Thread.sleep(10000)

    val blocks = nodes.map { n =>

      val disableConsensusResponse = n.rpc.get("disableConsensus")
      assert(disableConsensusResponse.get().status == StatusCodes.OK)

      Thread.sleep(1000)

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

   //   assert(transactions == expectedTransactions)
    })

    nodes.foreach{
      _.shutdown()
    }

    assert(true)
  }

}
