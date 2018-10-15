package org.constellation

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ForkJoinPool}

import akka.actor.{ActorRef, Props}
import akka.stream.ActorMaterializer
import better.files.File
import constellation._
import org.constellation.ClusterTest.getPodMappings
import org.constellation.RestartCluster.system
import org.constellation.consensus.{EdgeProcessor, MemPool, Snapshot, SnapshotInfo}
import org.constellation.primitives.{IncrementMetric, MetricsManager, PeerManager}
import org.constellation.primitives.Schema.{CheckpointBlock, Sheaf, Transaction}
import org.constellation.util.APIClient

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object DownloadChainComputeSingle {



  def doDownload(tips: Seq[CheckpointBlock], a1: APIClient) = { //(implicit ec: ExecutionContext) = {

   // implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

    implicit val ec = a1.system.dispatcher

    val cbs = TrieMap[String, CheckpointBlock]()

    @volatile var count = 0

    val parentsAwaitingDownload = new ConcurrentLinkedQueue[String]()
    val threadsFinished = TrieMap[Int, Boolean]()

    tips.foreach{ z =>
      cbs(z.baseHash) = z
      z.parentSOEBaseHashes.foreach{h =>
        parentsAwaitingDownload.add(h)
      }
    }

    val par = Seq.fill(100)(0)
    val res = par.map{ i =>

      Future {
        threadsFinished(i) = false

        do {

          val hash = parentsAwaitingDownload.poll()
          if (hash != null) {
            threadsFinished(i) = false
            if (!cbs.contains(hash)) {
              val str = "checkpoint/" + hash
              println(str)
              val value = a1.getSync(str)
              println(value)
              val cbo = value.body.x[Option[CheckpointBlock]]
              if (cbo.nonEmpty) {
                val cb = cbo.get
                if (!cbs.contains(cb.baseHash)) {
                  cbs(cb.baseHash) = cb
                  count += 1
                  if (count % 100 == 0) {
                    println(count)
                  }
                  if (!cb.parentSOE.contains(null)) {
                    cb.parentSOEBaseHashes.foreach {
                      parentsAwaitingDownload.add
                    }
                  }
                }
              }
            }
          } else {
            threadsFinished(i) = true
          }

        } while (threadsFinished.exists(_._2 == false))

      }
    }

    Future.sequence(res).get(1000)

    println("Total number of CBs downloaded " + cbs.size)
    println("Done")
  }



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

   // val tips = a1.getBlocking[Seq[CheckpointBlock]]("tips")

  //  doDownload(tips, a1)
    val snapshotInfo = a1.getBlocking[SnapshotInfo]("info")


    def getSnapshots(hash: String, blocks: Seq[String] = Seq()): Seq[String] = {
      val sn = a1.getBlocking[Option[Snapshot]]("snapshot/" + hash)
      if (sn.isEmpty) {
        println("empty snapshot")
        blocks
      } else {
        val next = sn.get.lastSnapshot
        println("next " + next + " num: " + sn.get.checkpointBlocks.size)
        if (next == "") blocks
        else {
          getSnapshots(next, blocks ++ sn.get.checkpointBlocks)
        }
      }
    }

    val snapshotBlocks = getSnapshots(snapshotInfo.snapshot.lastSnapshot) ++ snapshotInfo.acceptedCBSinceSnapshot

    println(snapshotBlocks.size)
    println(snapshotInfo.snapshot.lastSnapshot)


   // println(info)

    /*
        val dao = new DAO()

        // Setup actors
        val metricsManager: ActorRef = system.actorOf(
          Props(new MetricsManager(dao)), s"MetricsManager"
        )

        val peerManager: ActorRef = system.actorOf(
          Props(new PeerManager(dao)), s"PeerManager"
        )
    */

    system.terminate()


/*

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
*/

    // val res = processAncestor2(tips.head.parentSOEBaseHashes.head)
    /*
        val res = Seq(tips.head).flatMap{ z =>
          cbs(z.baseHash) = z
          z.parentSOEBaseHashes.map{processAncestor2}
        }*/


    //res.get()
    // val done = Future.sequence(res).get(1000)


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


    /*

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
    */




  }
}
