package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import constellation._
import org.constellation.ClusterDebug.system
import org.constellation.ClusterTest.getPodMappings
import org.constellation.primitives.Schema._
import org.constellation.util.APIClient

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
      val r = new APIClient().setConnection(ip, 9000)
      r
    }

    val nodeIp = "35.225.201.13"

    val a1 = apis.filter{_.hostName == nodeIp}.head

    val chainFile = File("chain.jsonl")
    chainFile.delete(true)

    val m = a1.getBlocking[Metrics]("metrics").metrics

    val genesisBundleHash = m("z_genesisBundleHash")
    var hash = m("lastValidBundleHash")

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
          chainFile.appendLine(s.json)
      }
    }

    println("Done")
    system.terminate()
  }
}
