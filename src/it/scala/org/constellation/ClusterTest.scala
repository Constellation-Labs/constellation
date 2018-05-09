package org.constellation

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.util.RPCClient
import org.json4s.JsonAST.JArray
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{ExecutionContextExecutor, Future}
import constellation._
import org.constellation.ClusterTest.{KubeIPs, ipRegex}
import org.constellation.p2p.PeerToPeer.Peer
import org.constellation.primitives.Block

import scala.sys.process._
import scala.util.Try


object ClusterTest {

  private val ipRegex = "\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b".r
  private val isCircle = System.getenv("CIRCLE_SHA1") != null
  val kubectl: Seq[String] = if (isCircle) Seq("sudo", "/opt/google-cloud-sdk/bin/kubectl") else Seq("kubectl")

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

    rpcs.foreach { rpc =>
      Future {
        val genResponse1 = rpc.get("generateGenesisBlock")
        assert(genResponse1.get().status == StatusCodes.OK)
      }
    }

    Thread.sleep(5000)

    for (rpc1 <- rpcs) {

      val chainStateNode1Response = rpc1.get("blocks")

      val response = chainStateNode1Response.get()

      println(s"ChainStateResponse $response")

      val chainNode1 = rpc1.read[Seq[Block]](response).get().toList

      println(s"Genesis: ${chainNode1.head}")

      assert(chainNode1.head.height == 0)

      assert(chainNode1.head.round == 0)

      assert(chainNode1.head.parentHash == "tempGenesisParentHash")

      assert(chainNode1.head.signature == "tempSig")

      assert(chainNode1.head.transactions == Seq())

      val consensusResponse1 = rpc1.get("enableConsensus")

      assert(consensusResponse1.get().status == StatusCodes.OK)

    }

    Thread.sleep(3000)

    import Fixtures2._

    val txs = Seq(transaction1, transaction2, transaction3, transaction4)

    txs.foreach{ tx =>
      val rpc1 = rpcs.head
      rpc1.post("transaction", tx)
    }

    Thread.sleep(6000)

    rpcs.foreach { rpc =>
      Future {
        val disableConsensusResponse1 = rpc.get("disableConsensus")
        assert(disableConsensusResponse1.get().status == StatusCodes.OK)
      }
    }

    Thread.sleep(3000)

    rpcs.foreach { rpc =>
      Future {
        val disableConsensusResponse1 = rpc.get("disableConsensus")
        assert(disableConsensusResponse1.get().status == StatusCodes.OK)
      }
    }

    Thread.sleep(1000)

    val blocks = rpcs.map{ n=>
      val finalChainStateNode1Response = n.get("blocks")
      val finalChainNode1 = n.read[Seq[Block]](finalChainStateNode1Response.get()).get()
      finalChainNode1
    }

    print("Block lengths : " +blocks.map{_.length})

    val chainSizes = blocks.map{_.length}
    val totalNumTrx = blocks.flatMap(_.flatMap(_.transactions)).length

    print("chain sizes : " + chainSizes)

    println(s"Total number of transactions: $totalNumTrx")

    blocks.foreach{ b =>
       assert(b.flatMap{_.transactions}.size == (rpcs.length * 2))
    }

    val minSize = blocks.map(_.length).min

    assert(blocks.map{_.slice(0, minSize)}.distinct.size == 1)
    assert(totalNumTrx == (rpcs.length * 2 * rpcs.length))

    assert(true)
  }

}
