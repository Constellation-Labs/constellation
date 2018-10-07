package org.constellation

import java.util.concurrent.ConcurrentLinkedQueue

import akka.stream.ActorMaterializer
import better.files.File
import constellation._
import org.constellation.ClusterTest.getPodMappings
import org.constellation.RestartCluster.system
import org.constellation.primitives.Schema.{CheckpointBlock, Sheaf, Transaction}
import org.constellation.util.APIClient

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.ExecutionContext.Implicits.global

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

    val cbs = TrieMap[String, CheckpointBlock]()

    val parentsNeedingToBeDownloaded = new ConcurrentLinkedQueue[String]()
    val hashesBeingDownloaded = new ConcurrentLinkedQueue[String]()

    @volatile var count = 0

/*
    tips.foreach{ z =>
      cbs(z.baseHash) = z
      z.parentSOEBaseHashes.foreach{clq.add}
    }
    */
/*
    Seq.fill(10)(0).par.foreach{
      _ =>

        while (clq.poll() && noRecentResponses)
    }

*/


    def processAncestor2(h: String): Future[Unit] = {
      if (!cbs.contains(h)) {
        a1.get("checkpoint/" + h).map {
          _.body.x[Option[CheckpointBlock]]
        }.map {
          _.map { cb =>
            if (!cbs.contains(cb.baseHash)) {
              cbs(cb.baseHash) = cb
              count += 1
              if (count % 100 == 0) {
                println(count)
              }
              if (!cb.parentSOE.contains(null)) {
                Future.sequence(cb.parentSOEBaseHashes.map(processAncestor2))
              } else Future.unit
            } else Future.unit
          }.getOrElse(Future.unit)
        }
      } else Future.unit
    }



    def processAncestor(h: String): Unit = {
      if (!cbs.contains(h)) {
        a1.getBlocking[Option[CheckpointBlock]]("checkpoint/" + h).foreach{ cb =>
          if (!cbs.contains(cb.baseHash)) {
            cbs(cb.baseHash) = cb
            count += 1
            if (count % 100 == 0) {
              println(count)
            }
            if (!cb.parentSOE.contains(null)) {
              cb.parentSOEBaseHashes.par.foreach(processAncestor)
            }
          }
        }
      }
    }

    tips.par.foreach{ z =>
      cbs(z.baseHash) = z
      z.parentSOEBaseHashes.par.foreach{processAncestor2}
    }



    println("Total number of CBs downloaded" + cbs.size)


    println("Done")
    system.terminate()



  }
}
