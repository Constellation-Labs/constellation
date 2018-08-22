package org.constellation

import akka.stream.ActorMaterializer
import constellation._
import org.constellation.RestartCluster.system
import org.constellation.ClusterTest.getPodMappings
import org.constellation.primitives.Schema.{BundleHashQueryResponse, MetricsResult}
import org.constellation.util.{APIClient, Simulation}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object UploadChain {

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def getPods(statefulSetId: String): List[ClusterTest.PodIPName] = {


    constellation.makeKeyPair()
    val mappings = getPodMappings(statefulSetId)
    mappings
  }

  def main(args: Array[String]): Unit = {
/*

    val mappings = getPodMappings("constellation-app-ryle")

    val ips = mappings.map {
      _.externalIP
    }

    val apis = ips.map { ip =>
      val r = new APIClient(ip, 9000)
      r
    }

    val sim = new Simulation(apis)
    sim.setExternalIP()

    val a1 = apis.head
    apis.map {
      _.host
    }.foreach {
      println
    }
*/


    val mappings2 = getPodMappings("constellation-app")
    val apisProd = mappings2.map { z => new APIClient().setConnection(z.externalIP, 9000) }

    val mp = apisProd.map {
      _.getBlocking[MetricsResult]("metrics").metrics
    }

   // val m = a1.getBlocking[Metrics]("metrics").metrics

    mp.map {
      _ ("z_validLedger")
    }.foreach {
      println
    }
    //mp.foreach{println}
    //println(m)
   // println(m("z_validLedger"))

    //  a1.get("restart")

    val chainLines = scala.io.Source.fromFile("chain.jsonl").getLines().toSeq.reverse

    val ledger = TrieMap[String, Long]()

    var numTX = 0

    var hashes = Set[String]()


    chainLines.foreach{ l =>
      l.x[BundleHashQueryResponse].transactions.foreach{
        t =>
          if (!hashes.contains(t.hash)) {
            t.txData.data.updateLedger(ledger)
            numTX += 1
            hashes += t.hash
          }
      }
    }

    println(ledger.toMap.json)
    println(numTX)
    println(chainLines.size)

    /*

    */
    /*
    a1.getSync("restart")

    Thread.sleep(5000)


    chainLines.grouped(10).foreach{
      c =>
        val cj = c.map{_.x[BundleHashQueryResponse]}
//        println("uploading")
        println(cj.head.sheaf.get.height.get)
        a1.postSync("upload", cj)
    }

    a1.postSync("completeUpload", chainLines.last.x[BundleHashQueryResponse])


    mappings2.map{
      m =>
        m.externalIP + ":16180"
    }.foreach{ z =>
      println(a1.postSync("peer", z))
    }

  }*/


    println("Done")
    system.terminate()
  }
}
