package org.constellation

import akka.stream.ActorMaterializer
import org.constellation.ClusterDebug.system
import org.constellation.ClusterTest.getPodMappings
import org.constellation.primitives.Schema.{Sheaf, Transaction}
import org.constellation.util.APIClient
import constellation._

import scala.concurrent.ExecutionContextExecutor

object DownloadChain {


  def main(args: Array[String]): Unit = {

    implicit val materialize: ActorMaterializer = ActorMaterializer()

    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    constellation.makeKeyPair()


    val clusterId = sys.env.getOrElse("CLUSTER_ID", "constellation-app")

    val mappings = getPodMappings(clusterId)

    mappings.foreach{println}

    val ips = mappings.map{_.externalIP}

    val apis = ips.map{ ip =>
      val r = new APIClient(ip, 9000)
      r
    }

    val a1 = apis.head

    var hash = "176a2de889a783ba735f378e88e165f63f360eec7e62d69e642fae84cbe5b53f"

    var chain: Seq[Sheaf] = Seq()

    var transactions : Seq[Transaction] = Seq()

    while (hash != "coinbase") {
      val sheaf = a1.getBlocking[Option[Sheaf]]("bundle/" + hash)
      println("Got sheaf " + sheaf.get.height.get + " " + hash)
      hash = sheaf.get.bundle.extractParentBundleHash.pbHash
      chain = Seq(sheaf.get) ++ chain
    }


    println("Chain size: " + chain.size)

    scala.tools.nsc.io.File("chain.jsonl").writeAll(chain.map{_.json + "\n"}:_*)

    println("Done")
  }
}
