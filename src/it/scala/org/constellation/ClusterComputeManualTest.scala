package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import better.files._
import org.constellation.crypto.KeyUtils
import org.constellation.util.{APIClient, Simulation}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object ComputeTestUtil {

  // For custom deployments to non-GCP instances
  // When deploy script is better this can go away. Was used for testing on home computer
  def getAuxiliaryNodes(startMultiNodeMachines: Boolean = false)(
    implicit as: ActorSystem,
    mat: ActorMaterializer,
    ec: ExecutionContextExecutor
  ): (Seq[String], Seq[APIClient]) = {

    var ignoreIPs = Seq[String]()

    val auxAPIs = Try {
      file"aux-hosts.txt".lines.toSeq
    }.getOrElse(Seq()).map { ip =>
      val split = ip.split(":")
      val host = split.head
      val str = host + ":" + split(1)
      ignoreIPs :+= str
      val offset = split(2).toInt
      println(s"Initializing API to $str offset: $offset")
      APIClient(
        split.head,
        port = offset + 1,
        peerHTTPPort = offset + 2,
        internalPeerHost = split(3)
      )
    }

    val auxMultiAPIs = Try { file"aux-multi-host.txt".lines.toSeq }.getOrElse(Seq()).flatMap { ip =>
      val split = ip.split(":")
      val host = split.head
      val str = host + ":" + split(1)
      val offset = split(2).toInt + 2
      Seq.tabulate(3) { i =>
        val adjustedOffset = offset + i * 2
        println(s"Initializing API to $str offset: $adjustedOffset")
        if (startMultiNodeMachines) {
          import scala.sys.process._
          val javaCmd = s"java -jar ~/dag.jar $adjustedOffset > ~/dag-$i.log 2>&1 &"
          val sshCmd = Seq("ssh", host, s"""bash -c '$javaCmd'""")
          println(sshCmd.mkString(" "))
          println(sshCmd.!!)
        }
        APIClient(
          split.head,
          port = adjustedOffset + 1,
          peerHTTPPort = adjustedOffset + 2,
          internalPeerHost = split(3)
        )

      }
    }

    ignoreIPs -> (auxAPIs ++ auxMultiAPIs)

  }

  def createApisFromIpFile(filePath: String, ignoreIPs: Seq[String])(implicit ec: ExecutionContextExecutor): Seq[APIClient] = {
    val ips = file"$filePath".lines.toSeq.filterNot(ignoreIPs.contains)

    Simulation.logger.info(ips.toString)

    val apis = ips.map { ip =>
      val split = ip.split(":")
      val portOffset = if (split.length == 1) 8999 else split(1).toInt
      val a = APIClient(split.head, port = portOffset + 1, peerHTTPPort = portOffset + 2)
      Simulation.logger.info(
        s"Initializing API to ${split.head} ${portOffset + 1} ${portOffset + 2}"
      )
      a
    }
    Simulation.logger.info("Num APIs " + apis.size)
    apis
  }

}

/**
  * Main integration test / node initializer / cluster startup script.
  *
  * Several API calls in here should be part of the regular node initialization, so this
  * test should do less.
  *
  * We also can't make this a main method in the main folder (for the regular init API calls as opposed to the test calls)
  * so this needs to be split up at some point. Putting another main in the regular classpath causes an issue with
  * sbt docker image, needs to be fixed and then portions of this can be split into separate mains for init methods
  * vs actual test.
  */
class ClusterComputeManualTest
    extends TestKit(ActorSystem("ClusterTest"))
    with FlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  // For fixing some old bug, revisit later if necessary
  KeyUtils.makeKeyPair()

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {

    // Unused for standard tests, only for custom ones
    val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()

    val primaryHostsFile = System.getenv().getOrDefault("HOSTS_FILE", "hosts-2.txt")

    Simulation.logger.info(s"Using primary hosts file: $primaryHostsFile")

    val apis = ComputeTestUtil.createApisFromIpFile(primaryHostsFile, ignoreIPs)

    assert(Simulation.checkHealthy(apis))

    Simulation.setIdLocal(apis)
    val addPeerRequests = apis.map { a =>
      val aux = if (auxAPIs.contains(a)) a.internalPeerHost else ""
      PeerMetadata(
        a.hostName,
        a.peerHTTPPort,
        a.id,
        auxHost = aux,
        resourceInfo = ResourceInfo(diskUsableBytes = 1073741824)
      )
    }

    Simulation.run(apis, addPeerRequests, attemptSetExternalIP = true, useRegistrationFlow = true)

    // For debugging / adjusting options after compile
    /*
        println(apis.map{
          _.postSync(
            "config/update",
            ProcessingConfig(maxWidth = 10, minCheckpointFormationThreshold = 10, minCBSignatureThreshold = 3)
          )
        })
   */

  }

}
