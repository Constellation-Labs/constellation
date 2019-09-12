package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.util.APIClient
import org.json4s.JsonAST.JArray
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.sys.process._
import scala.util.Try

object ClusterTest {

  private val ipRegex = "\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b".r

  private def isCircle = System.getenv("CIRCLE_SHA1") != null

  def kubectl: Seq[String] =
    if (isCircle) Seq("sudo", "/opt/google-cloud-sdk/bin/kubectl") else Seq("kubectl")

  case class KubeIPs(id: Int, rpcIP: String, udpIP: String) {

    def valid: Boolean =
      ipRegex.findAllIn(rpcIP).nonEmpty && ipRegex.findAllIn(udpIP).nonEmpty
  }

  // todo: Add documentation.

  @deprecated(
    "Use node IPs for now -- this was for previous tests but may be useful later.",
    "a few months"
  )
  def getServiceIPs: List[KubeIPs] = {
    val cmd = kubectl ++ Seq("--output=json", "get", "services")
    val result = cmd.!!
    // println(s"GetIP Result: $result")
    val items = (result.jValue \ "items").extract[JArray]

    val namedIPs = items.arr.flatMap { i =>
      val name = (i \ "metadata" \ "name").extract[String]

      if (name.contains("rpc") || name.contains("udp")) {
        val ip = ((i \ "status" \ "loadBalancer" \ "ingress").extract[JArray].arr.head \ "ip")
          .extract[String]
        Some(name -> ip)
      } else None
    }
    namedIPs
      .groupBy(_._1.split("-").last.toInt)
      .map {
        case (k, vs) =>
          KubeIPs(
            k,
            vs.filter {
              _._1.startsWith("rpc")
            }.head._2,
            vs.filter {
              _._1.startsWith("udp")
            }.head._2
          )
      }
      .toList
  }

  case class NodeIPs(internalIP: String, externalIP: String)

  def getNodeIPs: Seq[NodeIPs] = {
    val result = { kubectl ++ Seq("get", "-o", "json", "nodes") }.!!
    val items = (result.jValue \ "items").extract[JArray]
    val res = items.arr.flatMap { i =>
      val kind = (i \ "kind").extract[String]
      if (kind == "Node") {

        val externalIP = (i \ "status" \ "addresses")
          .extract[JArray]
          .arr
          .collectFirst {
            case x if (x \ "type").extract[String] == "ExternalIP" =>
              (x \ "address").extract[String]
          }
          .get
        val internalIP = (i \ "status" \ "addresses")
          .extract[JArray]
          .arr
          .collectFirst {
            case x if (x \ "type").extract[String] == "Hostname" =>
              (x \ "address").extract[String]
          }
          .get
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
      //  val hostIPInternal = (p \ "status" \ "hostIP").extract[String]
      val hostIPInternal = (p \ "spec" \ "nodeName").extract[String]
      val externalIP = nodes.collectFirst {
        case x if x.internalIP == hostIPInternal => x.externalIP
      }.get
      PodIPName((p \ "metadata" \ "name").extract[String], hostIPInternal, externalIP)
    }

    hostIPToName
  }

}

// TODO: Re-enable after doing kubernetes entropy / haveged fix

class ClusterTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val backend: SttpBackend[Future, Nothing] =
    PrometheusBackend[Future, Nothing](OkHttpFutureBackend()(ConstellationExecutionContext.unbounded))

  import ClusterTest._

  private val kp = KeyUtils.makeKeyPair()

  private val clusterId = sys.env.getOrElse("CLUSTER_ID", "constellation-app")

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {

    println("Grabbing cluster STS : " + clusterId)

    val mappings = getPodMappings(clusterId)

    mappings.foreach { println }

    val ips = mappings.map { _.externalIP }

    val apis = ips.map { ip =>
      APIClient(ip, 9000)
    }

    /*
    val splitApis = apis.splitAt(1)

    val initialApis = splitApis._2

    val newApi = splitApis._1(0)

    println("initialApis = ", initialApis)

    println("newApi = ", newApi)
     */

    val peerAPIs = ips.map { ip =>
      APIClient(ip, 9001)
    }

    // sim.run(apis = apis, peerApis = peerAPIs, attemptSetExternalIP = true)

  }

}
