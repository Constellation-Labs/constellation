package org.constellation

import java.util.concurrent.ForkJoinPool

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.util.APIClient
import org.json4s.JsonAST.JArray
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import constellation._
import org.constellation.ClusterTest.{KubeIPs, ipRegex}
import org.constellation.primitives.Schema.{Address, SendToAddress, TX}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.primitives.Schema._

import scala.sys.process._
import scala.util.{Random, Try}
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

  // TODO: disabling until we port over to our new consensus mechanism
  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {

    val mappings = getPodMappings(clusterId)

    mappings.foreach{println}

    val ips = mappings.map{_.externalIP}

    val rpcs = ips.map{ ip =>
      val r = new APIClient(ip, 9000)
      assert(r.getBlockingStr[String]("health", timeout = 100) == "OK")
      println(s"Health ok on $ip")
      val id = r.getBlocking[Id]("id")
      r.id = id
      r
    }

    for (rpc  <- rpcs) {
      assert(rpc.post("ip", rpc.host + ":16180").get().unmarshal.get() == "OK")
    }

    val r1 = rpcs.head

    // Create a genesis transaction
    val numCoinsInitial = 4e9.toLong
    val genTx = r1.getBlocking[TX]("genesis/" + numCoinsInitial)

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

    val initialDistrTX = rpcs.tail.map{ n =>
      val dst = n.getBlocking[Address]("selfAddress")
      val s = SendToAddress(dst, 1e7.toLong)
      r1.postRead[TransactionQueryResponse]("sendToAddress", s).tx.get
    }

    Thread.sleep(10000)

    def randomNode = rpcs(Random.nextInt(rpcs.length))
    def randomOtherNode(not: APIClient) = rpcs.filter{_ != not}(Random.nextInt(rpcs.length - 1))

    val ec = ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    def sendRandomTransaction = {
      Future {
        val src = randomNode
        val dst = randomOtherNode(src).getBlocking[Address]("selfAddress")
        val s = SendToAddress(dst, Random.nextInt(1000).toLong)
        src.postRead[TransactionQueryResponse]("sendToAddress", s).tx.get
      }(ec)
    }

    val numTX = 200

    val start = System.currentTimeMillis()

    val txResponse = Seq.fill(numTX) {
      // Thread.sleep(100)
      sendRandomTransaction
    }

    val txResponseFut = Future.sequence(txResponse)

    val txs = txResponseFut.get(100).toSet

    val allTX = Set(genTx) ++ initialDistrTX.toSet ++ txs

    var done = false

    while (!done) {
      val nodeStatus = rpcs.map { n =>
        val validTX = n.getBlocking[Set[TX]]("validTX")
        val percentComplete = 100 - (allTX.diff(validTX).size.toDouble / allTX.size.toDouble) * 100
        println(s"Node ${n.id.short} validTXSize: ${validTX.size} allTXSize: ${allTX.size} % complete: $percentComplete")
        Thread.sleep(1000)
        validTX == allTX
      }

      if (nodeStatus.forall { x => x }) {
        done = true
      }
    }

    val end = System.currentTimeMillis()

    println(s"Completion time seconds: ${(end-start) / 1000}")

  }

}
