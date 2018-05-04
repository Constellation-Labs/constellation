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
import org.constellation.p2p.PeerToPeer.Peer
import org.constellation.primitives.{Block, Transaction}

import scala.util.Try


class ClusterTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val isCircle = System.getenv("CIRCLE_SHA1") != null

  def getIPs: List[String] = {
    import scala.sys.process._

    val cmd = {if (isCircle) Seq("sudo", "/opt/google-cloud-sdk/bin/kubectl") else Seq("kubectl")} ++
      Seq("--output=json", "get", "services")
    val result = cmd.!!
    // println(s"GetIP Result: $result")
    val items = (result.jValue \ "items").extract[JArray]
    val ips = items.arr.filter{ i =>
      (i \ "metadata" \ "name").extract[String].startsWith("rpc")
    }.map{ i =>
      ((i \ "status" \ "loadBalancer" \ "ingress").extract[JArray].arr.head \ "ip").extract[String]
    }
    ips
  }

  "Cluster" should "ping a cluster" in {


    if (isCircle) {
      println("Is circle, waiting for machines to come online")
      var done = false
      var i = 0

      while (!done) {
        i += 1
        Thread.sleep(30000)
        val t = Try{getIPs}
        println(s"Waiting for machines to come online $t ${i * 30} seconds")
        done = t.toOption.exists { ps =>
          val validIPs = ps.forall { ip =>
            val res = "\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b".r.findAllIn(ip)
            res.nonEmpty
          }
          ps.lengthCompare(3) == 0 && validIPs
        }
      }
      Thread.sleep(10000)
    }

    val ips = getIPs

    val rpcs = ips.map{
      ip =>
        val r = new RPCClient(ip, 9000)
        assert(r.getBlockingStr[String]("health", timeout = 100) == "OK")
        println(s"Health ok on $ip")
        assert(r.post("ip", ip + ":16180").get().unmarshal.get() == "OK")
        r
    }

    rpcs.head.get("master")

    for (rpc <- rpcs) {
      println(s"Trying to add nodes to $rpc")
      val others = rpcs.filter{_ != rpc}
      others.foreach{
        n =>
          Future {
            val peerStr = n.host + ":16180"
            val res = rpc.postSync("peer", peerStr)
            println(s"Trying to add $peerStr to $rpc res: $res")
          }
      }
    }

    Thread.sleep(25000)


/*

    Thread.sleep(5000)

    for (rpc <- rpcs) {
      val peers = rpc.getBlocking[Seq[Peer]]("peerids")
      println(s"Peers length: ${peers.length}")
      assert(peers.length == (rpcs.length - 1))
    }

    for (rpc <- rpcs) {

      val genResponse1 = rpc.get("generateGenesisBlock")
      assert(genResponse1.get().status == StatusCodes.OK)

    }

    Thread.sleep(2000)

    for (rpc1 <- rpcs) {

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

      val consensusResponse1 = rpc1.get("enableConsensus")

      assert(consensusResponse1.get().status == StatusCodes.OK)

    }

    Thread.sleep(5000)

    import Fixtures._

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
