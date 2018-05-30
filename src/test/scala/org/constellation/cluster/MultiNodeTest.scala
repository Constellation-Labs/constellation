package org.constellation.cluster

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent._

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
import org.constellation.util.APIClient
import org.constellation.util.TestNode
import org.scalatest.exceptions.TestFailedException
import sun.security.provider.NativePRNG.Blocking

import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Random._

class MultiNodeTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
  with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(180, TimeUnit.SECONDS)

  def getRandomNode(nodes: Seq[ConstellationNode]): ConstellationNode = {
    shuffle(nodes).head
  }

  def generateTransactions(nodes: Seq[ConstellationNode], numberOfTransactions: Int): Seq[Transaction] = {
    Range(0, numberOfTransactions).map(f => {

      val sender = getRandomNode(nodes)

      val receiver = getRandomNode(nodes.filterNot(n => n == sender))

      Transaction.senderSign(Transaction(0L, sender.id.id,
        receiver.id.id, nextInt(10000).toLong), sender.keyPair.getPrivate)
    })
  }

  // TODO: disabling until we port over to our new consensus mechanism
  "E2E Multiple Nodes" should "add peers and build blocks with transactions" ignore {

    val numberOfNodes = 3

    val nodes = Seq.fill(numberOfNodes)(TestNode())

    val numberOfTransactions = 20

    val expectedTransactions = generateTransactions(nodes, numberOfTransactions)

    val slideNum = numberOfTransactions / numberOfNodes

    val randomizedTransactions = shuffle(expectedTransactions).toList.sliding(slideNum, slideNum).toList

    assert(expectedTransactions.length == numberOfTransactions)

    val initPeersFutures = nodes.map(node => {
      Future {

        assert(node.healthy)

        for (n1 <- nodes) {
          println(s"Trying to add nodes to $n1")

          val others = nodes.filter {
            _ != n1
          }

          others.foreach { n => {
            n1.add(n)
            println(s"Trying to add $n to $n1")
          }
          }

        }

        val peers = node.api.getBlocking[Seq[Peer]]("peerids")
        println(s"Peers length: ${peers.length}")
        assert(peers.length == (nodes.length - 1))

        true
      }
    })

    val initPeersSequence = Future.sequence(initPeersFutures)

    Await.result(initPeersSequence, 30 seconds)

    val initGenesisFutures = nodes.map(node => {
      Future {

        val rpc = node.api
        val genResponse = rpc.get("generateGenesisBlock")
        assert(genResponse.get().status == StatusCodes.OK)

        true
      }
    })

    val initGenesisSequence = Future.sequence(initGenesisFutures)

    Await.result(initGenesisSequence, 30 seconds)

    val validateGenesisBlockFutures = nodes.map(node => {
      Future {

        val rpc = node.api

        val chainStateNode1Response = rpc.get("blocks")

        val response = chainStateNode1Response.get()

        val chainNode = rpc.read[Seq[Block]](response).get()

        assert(chainNode.size == 1)

        assert(chainNode.head.height == 0)

        assert(chainNode.head.round == 0)

        assert(chainNode.head.parentHash == "tempGenesisParentHash")

        assert(chainNode.head.signature == "tempSig")

        assert(chainNode.head.transactions == Seq())

        assert(chainNode.head.clusterParticipants.diff(nodes.map {_.id}.toSet).isEmpty)

        val consensusResponse = rpc.get("enableConsensus")

        assert(consensusResponse.get().status == StatusCodes.OK)

        true
      }
    })

    val validateGenesisBlockSequence = Future.sequence(validateGenesisBlockFutures)

    Await.result(validateGenesisBlockSequence, 30 seconds)

    val makeTransactionsFutures = nodes.zipWithIndex.map { case (node, index) => {
      Future {
        var transactionCallsMade = Seq[Transaction]()

        val rpc = node.api

        def makeTransactionCalls(idx: Int): Unit = {
          if (randomizedTransactions.isDefinedAt(idx)) {
            randomizedTransactions(idx).foreach(transaction => {
              rpc.post("transaction", transaction)
              transactionCallsMade = transactionCallsMade.:+(transaction)
            })
          }
        }

        makeTransactionCalls(index)

        // If we are on the last node but there are still transactions left then send them to the last node
        if (index + 1 == nodes.length) {
          makeTransactionCalls(index + 1)
        }

        transactionCallsMade
      }
    }}

    val makeTransactionsSequence = Future.sequence(makeTransactionsFutures)

    Await.result(makeTransactionsSequence, 30 seconds)

    val waitUntilTransactionsExistInBlocksFutures = nodes.map(node => {
      Future {
        val rpc = node.api

        def chainContainsAllTransactions(): Boolean = {
          val finalChainStateNodeResponse = rpc.get("blocks")
          val finalChainNode = rpc.read[Seq[Block]](finalChainStateNodeResponse.get()).get()

          finalChainNode.flatMap(c => c.transactions).size == expectedTransactions.size
        }

        while(!chainContainsAllTransactions()) {
          true
        }

        true
      }
    })

    val waitUntilTransactionsExistSequence = Future.sequence(waitUntilTransactionsExistInBlocksFutures)

    Await.result(waitUntilTransactionsExistSequence, 180 seconds)

    val chainsMap: Map[Id, Seq[Block]] = nodes.map { n =>

      val disableConsensusResponse = n.api.get("disableConsensus")
      assert(disableConsensusResponse.get().status == StatusCodes.OK)

      val finalChainStateNodeResponse = n.api.get("blocks")
      val finalChainNode = n.api.read[Seq[Block]](finalChainStateNodeResponse.get()).get()
      n.id -> finalChainNode
    }.toMap

    val chains = chainsMap.values

    val smallestChainLength = chains.map(_.size).min

    val trimmedChains = chains.map(c => c.take(smallestChainLength))

    // validate that all of the chains from each node are the same
    assert(trimmedChains.forall(_ == trimmedChains.head))

    val transactions = chainsMap.map(f => {
      f._2.flatMap(b => b.transactions).toSeq
    })

    assert(transactions.forall(_.size == expectedTransactions.size))

    assert(expectedTransactions.toSet.size == expectedTransactions.size)

    assert(transactions.forall(_.toSet == expectedTransactions.toSet))

    nodes.foreach{
      _.shutdown()
    }

    assert(true)
  }

}
