package org.constellation

import akka.stream.ActorMaterializer
import org.constellation.ClusterDebug.system
import org.constellation.ClusterTest.getPodMappings
import org.constellation.primitives.Schema.{BatchBundleHashRequest, BundleHashQueryResponse, Sheaf, Transaction}
import org.constellation.util.APIClient
import constellation._

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

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
    Try{chainFile.delete()}

    val genesisBundleHash = "0376d95b00b738f8400e5ca6b7e860cf20bb254aaf9607d8a1b6c207a674c945"
    var hash = "cab09861629d2fc574aa3a669e5d98a10eb617a917628c3c926511e8dad1b20d"

    while (hash != genesisBundleHash) {
      val ancestors = a1.getBlocking[Seq[BundleHashQueryResponse]]("download/" + hash)
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
