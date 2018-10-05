package org.constellation

import akka.stream.ActorMaterializer
import better.files.File
import constellation._
import org.constellation.ClusterTest.getPodMappings
import org.constellation.RestartCluster.system
import org.constellation.primitives.Schema.{CheckpointBlock, Sheaf, Transaction}
import org.constellation.util.APIClient

import scala.concurrent.ExecutionContextExecutor

object DownloadChainComputeSingle {

  def main(args: Array[String]): Unit = {

    implicit val materialize: ActorMaterializer = ActorMaterializer()

    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    constellation.makeKeyPair()


    import better.files._

    val ips = file"hosts.txt".lines.toSeq

    println(ips)

    val apis = ips.map{ ip =>
      val r = new APIClient().setConnection(ip, 9000)
      r
    }

    val a1 = apis.head

    val tips = a1.getBlocking[Seq[CheckpointBlock]]("tips")

    tips.foreach{
      z =>
        println(z.signatures.size)
    }

    println("Done")
    system.terminate()
  }
}
