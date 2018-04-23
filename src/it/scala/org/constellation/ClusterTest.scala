package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.util.RPCClient
import org.json4s.JsonAST.JArray
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor
import constellation._

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

    rpcs.foreach{
      r =>
        ips.foreach { ip =>
          val res = r.post("peer", ip + ":16180").get().unmarshal.get()
          println("Peer response: " + res)

        }
    }




  }


}
