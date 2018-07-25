package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import constellation._
import org.constellation.ClusterTest.getPodMappings
import org.constellation.RestartCluster.system
import org.constellation.primitives.Schema.{Sheaf, Transaction}
import org.constellation.util.APIClient

import scala.concurrent.ExecutionContextExecutor

object DownloadChainSingle {


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

    val nodeIp = "35.238.105.67"

    val a1 = apis.filter{_.host == nodeIp}.head

    val chainFile = File("single-chain.jsonl")
    chainFile.delete()
    val txFile = File("transactions.jsonl")
    txFile.delete()
    var hash = "9ead11ba079bf2f789c9207bdfab7779fd807ba4c2a2571326fc9bb6ce39365e"

    while (hash != "coinbase") {
      val sheaf = a1.getBlocking[Option[Sheaf]]("bundle/" + hash).get
      hash = sheaf.bundle.extractParentBundleHash.pbHash
      println(s"Height: ${sheaf.height.get} hash: $hash")
      sheaf.bundle.extractTXHash.foreach{ h =>
        val hActual = h.txHash
        val tx = a1.getBlocking[Option[Transaction]]("transaction/" + hActual)
        if (tx.isEmpty) println(s"Missing TX Hash: $hActual")
        txFile.appendLine(tx.get.json)
      }
      chainFile.appendLine(sheaf.json)
    }

    println("Done")
    system.terminate()
  }
}
