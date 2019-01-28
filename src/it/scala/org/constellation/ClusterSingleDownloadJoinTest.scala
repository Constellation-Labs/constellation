package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import better.files._

import org.constellation.consensus.{SnapshotInfo, StoredSnapshot} // currently unused // tmp comment
import org.constellation.crypto.KeyUtils
import org.constellation.util.{APIClient, Simulation}

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import scala.concurrent.ExecutionContextExecutor

// doc
class ClusterSingleDownloadJoinTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  // doc
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  // For fixing some old bug, revisit later if necessary
  KeyUtils.makeKeyPair()

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {

    val sim = new Simulation()

    // Unused for standard tests, only for custom ones
    val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()

    val primaryHostsFile = System.getenv().getOrDefault("HOSTS_FILE", "hosts-3.txt")

    sim.logger.info(s"Using primary hosts file: $primaryHostsFile")

    val ips = file"$primaryHostsFile".lines.toSeq.filterNot(ignoreIPs.contains)

    sim.logger.info(ips.toString)

    val apis = ips.map { ip =>
      val split = ip.split(":")
      val portOffset = if (split.length == 1) 8999 else split(1).toInt
      val a = APIClient(split.head, port = portOffset + 1, peerHTTPPort = portOffset + 2)
      sim.logger.info(s"Initializing API to ${split.head} ${portOffset + 1} ${portOffset + 2}")
      a
    } // ++ auxAPIs

    sim.logger.info("Num APIs " + apis.size)

    assert(sim.checkHealthy(apis))

    sim.setExternalIP(apis)

    apis.foreach {
      a =>
        println(a.postSync("peer/add", HostPort("104.198.7.226", 9001)))
        println("starting download on: " + a.hostName)
        Thread.sleep(20 * 1000)
        println(a.postEmpty("download/start"))
        Thread.sleep(120 * 1000)
    }

  } // end test

} // end ClusterSingleDownloadJoinTest class

