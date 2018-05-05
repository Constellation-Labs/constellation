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

  case class KubeIPs(id: Int, rpcIP: String, udpIP: String) {
    def valid: Boolean =  {
      ipRegex.findAllIn(rpcIP).nonEmpty && ipRegex.findAllIn(udpIP).nonEmpty
    }
  }

}

class ClusterTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val isCircle = System.getenv("CIRCLE_SHA1") != null

  private val kubectl = if (isCircle) Seq("sudo", "/opt/google-cloud-sdk/bin/kubectl") else Seq("kubectl")

  private val kp = makeKeyPair()

  // Use node IPs for now -- this was for previous tests
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

  def getNodeIPs: Seq[String] = {
    val result = {kubectl ++ Seq("get", "-o", "json", "nodes")}.!!
    val items = (result.jValue \ "items").extract[JArray]
    val res = items.arr.flatMap{ i =>
      val kind =  (i \ "kind").extract[String]
      if (kind == "Node") {
        (i \ "status" \ "addresses").extract[JArray].arr.collectFirst{
          case x if (x \ "type").extract[String] == "ExternalIP" =>
            (x \ "address").extract[String]
        }
      } else None
    }
    res
  }

  "Cluster" should "ping a cluster" in {

    if (isCircle) {
      println("Is circle, waiting for machines to come online")
      var done = false
      var i = 0

      while (!done) {
        i += 1
        Thread.sleep(30000)
        val t = Try{getNodeIPs}
        println(s"Waiting for machines to come online $t ${i * 30} seconds")
        done = t.toOption.exists { ps =>
          val validIPs = ps.forall {z =>  ipRegex.findAllIn(z).nonEmpty}
          ps.lengthCompare(3) == 0 && validIPs
        }
      }
      Thread.sleep(10000)
    }

    val ips = getNodeIPs


    val rpcs = ips.map{ ip =>
      val r = new RPCClient(ip, 9000)
      assert(r.getBlockingStr[String]("health", timeout = 100) == "OK")
      println(s"Health ok on $ip")
      r
    }

/*
    for (rpc  <- rpcs) {
      assert(r.post("ip", ip + ":16180").get().unmarshal.get() == "OK")
    }

    Thread.sleep(5000)

    val r1 = rpcs.head

    r1.get("master")

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

    Thread.sleep(5000)

    val peers1 = rpcs.head.get("peerids").get()
    println(s"Peers1 : $peers1")

    for (rpc <- rpcs) {
      val peers = rpc.getBlocking[Seq[Peer]]("peerids")
      println(s"Peers length: ${peers.length}")
      assert(peers.length == (rpcs.length - 1))
    }

    for (rpc <- rpcs) {

      val genResponse1 = rpc.get("generateGenesisBlock")
      assert(genResponse1.get().status == StatusCodes.OK)

    }
*/

    Thread.sleep(2000)

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
/*
      val consensusResponse1 = rpc1.get("enableConsensus")

      assert(consensusResponse1.get().status == StatusCodes.OK)*/

    }
/*

    Thread.sleep(5000)

    import Fixtures2._

    val txs = Seq(transaction1, transaction2, transaction3, transaction4)

    txs.foreach{ tx =>
      val rpc1 = rpcs.head
      rpc1.post("transaction", tx)

    }

    Thread.sleep(6000)

    val blocks = rpcs.map{ n=>
      val finalChainStateNode1Response = n.get("blocks")
      val finalChainNode1 = n.read[Seq[Block]](finalChainStateNode1Response.get()).get()
      finalChainNode1
    }

    print("Block lengths : " +blocks.map{_.length})
    val chainSizes = blocks.map{_.length}
    val totalNumTrx = blocks.flatMap(_.flatMap(_.transactions)).length

    println(s"Total number of transactions: $totalNumTrx")


*/



    /*

        rpcs.foreach{
          r =>
            val genResponse1 = r.get("generateGenesisBlock")
            assert(genResponse1.get().status == StatusCodes.OK)
        }
    */


  }


}
