package org.constellation

import akka.stream.ActorMaterializer
import org.constellation.ClusterDebug.system
import org.constellation.ClusterTest.getPodMappings
import org.constellation.primitives.Schema.{BatchBundleHashRequest, BundleHashQueryResponse, Sheaf, Transaction}
import org.constellation.util.APIClient
import constellation._

import scala.concurrent.ExecutionContextExecutor

object DownloadChainBatch {


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

    val chainFile = scala.tools.nsc.io.File("chain.jsonl")
    var hash = "9ead11ba079bf2f789c9207bdfab7779fd807ba4c2a2571326fc9bb6ce39365e"

    while (hash != "coinbase") {
      val ancestors = a1.getBlocking[Seq[BundleHashQueryResponse]]("bundleAll10/" + hash)
      val nonEmpty = ancestors.forall(_.sheaf.nonEmpty)
      val oldestHash = ancestors.head.hash
      val secondHasFirstAsParent = ancestors.tail.headOption.forall(_.sheaf.get.bundle.extractParentBundleHash.pbHash == oldestHash)
      println(s"Height: ${ancestors.head.sheaf.get.height.get} got size: ${ancestors.size} nonEmpty: $nonEmpty " +
        s"valid $secondHasFirstAsParent ancestors oldestHash: $oldestHash")
      hash = oldestHash
      ancestors.foreach{
        s =>
          chainFile.appendAll(s.json + "\n")
      }
    }

    println("Done")
    system.terminate()
  }
}
