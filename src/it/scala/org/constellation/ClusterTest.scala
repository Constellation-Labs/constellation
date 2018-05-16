package org.constellation

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.util.RPCClient
import org.json4s.JsonAST.JArray
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import constellation._
import org.constellation.ClusterTest.{KubeIPs, ipRegex}
import org.constellation.p2p.PeerToPeer.Peer
import org.constellation.primitives.{Block, Transaction}

import scala.sys.process._
import scala.util.Try
import scala.concurrent.duration._
import scala.util.Random._

object ClusterTest {

  private val ipRegex = "\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b".r
  private def isCircle = System.getenv("CIRCLE_SHA1") != null
  def kubectl: Seq[String] = if (isCircle) Seq("sudo", "/opt/google-cloud-sdk/bin/kubectl") else Seq("kubectl")

  case class KubeIPs(id: Int, rpcIP: String, udpIP: String) {
    def valid: Boolean =  {
      ipRegex.findAllIn(rpcIP).nonEmpty && ipRegex.findAllIn(udpIP).nonEmpty
    }
  }

  // Use node IPs for now -- this was for previous tests but may be useful later.
  @deprecated
  def getServiceIPs: List[KubeIPs] = {
    val cmd = kubectl ++ Seq("--output=json", "get", "services")
    val result = cmd.!!
    // println(s"GetIP Result: $result")
    val items = (result.jValue \ "items").extract[JArray]

    val namedIPs = items.arr.flatMap{ i =>
      val name =  (i \ "metadata" \ "name").extract[String]

      if (name.contains("rpc") || name.contains("udp")) {
        val ip = ((i \ "status" \ "loadBalancer" \ "ingress").extract[JArray].arr.head \ "ip").extract[String]
        Some(name -> ip)
      } else None
    }
    namedIPs.groupBy(_._1.split("-").last.toInt).map{
      case (k, vs) =>
        KubeIPs(k, vs.filter{_._1.startsWith("rpc")}.head._2,vs.filter{_._1.startsWith("udp")}.head._2)
    }.toList
  }

  case class NodeIPs(internalIP: String, externalIP: String)

  def getNodeIPs: Seq[NodeIPs] = {
    val result = {kubectl ++ Seq("get", "-o", "json", "nodes")}.!!
    val items = (result.jValue \ "items").extract[JArray]
    val res = items.arr.flatMap{ i =>
      val kind =  (i \ "kind").extract[String]
      if (kind == "Node") {

        val externalIP = (i \ "status" \ "addresses").extract[JArray].arr.collectFirst{
          case x if (x \ "type").extract[String] == "ExternalIP" =>
            (x \ "address").extract[String]
        }.get
        val internalIP = (i \ "status" \ "addresses").extract[JArray].arr.collectFirst{
          case x if (x \ "type").extract[String] == "InternalIP" =>
            (x \ "address").extract[String]
        }.get
        Some(NodeIPs(internalIP, externalIP))
      } else None
    }
    res
  }

  case class PodIPName(podAppName: String, internalIP: String, externalIP: String)

  def getPodMappings(namePrefix: String): List[PodIPName] = {

    val pods = ((kubectl ++ Seq("get", "-o", "json", "pods")).!!.jValue \ "items").extract[JArray]
    val nodes = getNodeIPs

    val hostIPToName = pods.filter { p =>
      Try {
        val name = (p \ "metadata" \ "name").extract[String]
        name.split("-").dropRight(1).mkString("-") == namePrefix
      }.getOrElse(false)
    }.map { p =>
      val hostIPInternal = (p \ "status" \ "hostIP").extract[String]
      val externalIP = nodes.collectFirst{case x if x.internalIP == hostIPInternal => x.externalIP}.get
      PodIPName((p \ "metadata" \ "name").extract[String], hostIPInternal, externalIP)
    }

    hostIPToName
  }

}

class ClusterTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  import ClusterTest._

  private val kp = makeKeyPair()

  private val clusterId = sys.env.getOrElse("CLUSTER_ID", "constellation-app")

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {

    val mappings = getPodMappings(clusterId)

    mappings.foreach{println}

    val ips = mappings.map{_.externalIP}

    val rpcs = ips.map{ ip =>
      val r = new RPCClient(ip, 9000)
      assert(r.getBlockingStr[String]("health", timeout = 100) == "OK")
      println(s"Health ok on $ip")
      r
    }

    for (rpc  <- rpcs) {
      assert(rpc.post("ip", rpc.host + ":16180").get().unmarshal.get() == "OK")
    }

    Thread.sleep(5000)

    for (rpc  <- rpcs) {
      val ip = rpc.host
      println(s"Trying to add nodes to $ip")
      val others = rpcs.filter{_.host != ip}.map{_.host + ":16180"}
      others.foreach{
        n =>
          Future {
            val res = rpc.postSync("peer", n)
            println(s"Tried to add peer $n to $ip res: $res")
          }
      }
    }

    Thread.sleep(3000)

    val peers1 = rpcs.head.get("peerids").get()
    println(s"Peers1 : $peers1")

    for (rpc <- rpcs) {
      val peers = rpc.getBlocking[Seq[Peer]]("peerids")
      println(s"Peers length: ${peers.length}")
      assert(peers.length == (rpcs.length - 1))
    }

    import Fixtures2._

    val txs = Seq(transaction1, transaction2, transaction3, transaction4)

    val slideNum = txs.size / rpcs.size

    val randomizedTransactions = shuffle(txs).toList.sliding(slideNum, slideNum).toList

    val initGenesisFutures = rpcs.map(rpc => {
      Future {

        val genResponse = rpc.get("generateGenesisBlock")
        assert(genResponse.get().status == StatusCodes.OK)

        true
      }
    })

    val initGenesisSequence = Future.sequence(initGenesisFutures)

    Await.result(initGenesisSequence, 30 seconds)

    val validateGenesisBlockFutures = rpcs.map(rpc => {
      Future {

        val chainStateNode1Response = rpc.get("blocks")

        val response = chainStateNode1Response.get()

        val chainNode = rpc.read[Seq[Block]](response).get()

        assert(chainNode.size == 1)

        assert(chainNode.head.height == 0)

        assert(chainNode.head.round == 0)

        assert(chainNode.head.parentHash == "tempGenesisParentHash")

        assert(chainNode.head.signature == "tempSig")

        assert(chainNode.head.transactions == Seq())

        val consensusResponse = rpc.get("enableConsensus")

        assert(consensusResponse.get().status == StatusCodes.OK)

        true
      }
    })

    val validateGenesisBlockSequence = Future.sequence(validateGenesisBlockFutures)

    Await.result(validateGenesisBlockSequence, 30 seconds)

    val makeTransactionsFutures = rpcs.zipWithIndex.map { case (rpc, index) => {
      Future {
        var transactionCallsMade = Seq[Transaction]()

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
        if (index + 1 == rpcs.length) {
          makeTransactionCalls(index + 1)
        }

        transactionCallsMade
      }
    }}

    val makeTransactionsSequence = Future.sequence(makeTransactionsFutures)

    Await.result(makeTransactionsSequence, 30 seconds)

    val waitUntilTransactionsExistInBlocksFutures = rpcs.map(rpc => {
      Future {

        def chainContainsAllTransactions(): Boolean = {
          val finalChainStateNodeResponse = rpc.get("blocks")
          val finalChainNode = rpc.read[Seq[Block]](finalChainStateNodeResponse.get()).get()

          val transactionsInChain = finalChainNode.flatMap(c => c.transactions).size

          println(s"chain length = ${finalChainNode.size}")

          println(s"expectedTransactions size = ${txs.size}")

          println(s"transactions in chain = $transactionsInChain")

          transactionsInChain == txs.size
        }

        while(!chainContainsAllTransactions()) {
          true
        }

        true
      }
    })

    val waitUntilTransactionsExistSequence = Future.sequence(waitUntilTransactionsExistInBlocksFutures)

    Await.result(waitUntilTransactionsExistSequence, 180 seconds)

    Thread.sleep(1000)

    val chains = rpcs.map { rpc =>
      val disableConsensusResponse = rpc.get("disableConsensus")
      assert(disableConsensusResponse.get().status == StatusCodes.OK)

      val finalChainStateNodeResponse = rpc.get("blocks")
      rpc.read[Seq[Block]](finalChainStateNodeResponse.get()).get()
    }

    val smallestChainLength = chains.map(_.size).min

    println(s"smallest chain length = $smallestChainLength")

    val trimmedChains = chains.map(c => c.take(smallestChainLength))

    // validate that all of the chains from each node are the same
    assert(trimmedChains.forall(_ == trimmedChains.head))

    val transactions = chains.map(f => {
      f.flatMap(b => b.transactions).toSeq
    })

    println(s"expected transactions size = ${txs.size}")

    assert(transactions.forall(_.size == txs.size))

    assert(txs.toSet.size == txs.size)

    assert(transactions.forall(_.toSet == txs.toSet))

    assert(true)

  }

}
