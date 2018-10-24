package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import constellation._
import org.constellation.util.{APIClient, Simulation}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try


class ClusterComputeManualTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val kp = makeKeyPair()

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {


    import better.files._



    var ignoreIPs = Seq[String]()


    val auxAPIs = Try{file"aux-hosts.txt".lines.toSeq}.getOrElse(Seq()).map{ ip =>
      val split = ip.split(":")
      val host = split.head
      val str = host + ":" + split(1)
      ignoreIPs :+= str
      val offset = split(2).toInt
      println(s"Initializing API to $str offset: $offset")
      new APIClient(split.head, port = offset + 1, peerHTTPPort = offset + 2, internalPeerHost = split(3))
    }

/*
    var startAuxMulti = true

    val auxMultiAPIs = Try{file"aux-multi-host.txt".lines.toSeq}.getOrElse(Seq()).flatMap{ ip =>
      val split = ip.split(":")
      val host = split.head
      val str = host + ":" + split(1)
      val offset = split(2).toInt + 2
      Seq.tabulate(3){i =>
        val adjustedOffset = offset + i*2
        println(s"Initializing API to $str offset: $adjustedOffset")
        if (startAuxMulti) {
          import scala.sys.process._
          val javaCmd = s"java -jar ~/dag.jar $adjustedOffset > ~/dag-$i.log 2>&1 &"
          val sshCmd = Seq("ssh", host, s"""bash -c '$javaCmd'""")
          println(sshCmd.mkString(" "))
          println(sshCmd.!!)
        }
        new APIClient(split.head, port = adjustedOffset + 1, peerHTTPPort = adjustedOffset + 2, internalPeerHost = split(3))

      }
    }
*/


   // val auxAPIs = Seq[APIClient]()
   // val auxMultiAPIs = Seq[APIClient]()

    val ips = file"hosts.txt".lines.toSeq.filterNot(ignoreIPs.contains)

    println(ips)

    val apis = ips.map{ ip =>
      val split = ip.split(":")
      val portOffset = if (split.length == 1) 8999 else split(1).toInt
      val a = new APIClient(split.head, port = portOffset + 1, peerHTTPPort = portOffset + 2)
      println(s"Initializing API to ${split.head} ${portOffset + 1} ${portOffset + 2}")
      a
    } ++ auxAPIs // ++ auxMultiAPIs

    println("Num APIs " + apis.size)

    val sim = new Simulation()

    assert(sim.checkHealthy(apis))

    sim.setIdLocal(apis)

    val addPeerRequests = apis.map{ a =>
      val aux = if (auxAPIs.contains(a)) a.internalPeerHost else ""
      AddPeerRequest(a.hostName, a.udpPort, a.peerHTTPPort, a.id, auxHost = aux)
    }

/*
    println(apis.map{
      _.postSync(
        "config/update",
        ProcessingConfig(maxWidth = 10, minCheckpointFormationThreshold = 10, minCBSignatureThreshold = 3)
      )
    })
*/

    sim.run(apis, addPeerRequests, attemptSetExternalIP = true)

/*
    sim.logger.info("Genesis validation passed")

    sim.triggerRandom(apis)

    sim.setReady(apis)

    assert(sim.awaitCheckpointsAccepted(apis))

    sim.logger.info("Checkpoint validation passed")

    assert(sim.checkSnapshot(apis))

    sim.logger.info("Snapshot validation passed")
*/


    /*
        println((apis.head.hostName, apis.head.apiPort))
        val goe = sim.genesis(apis)
        println(goe)
        apis.foreach{_.post("genesis/accept", goe)}
        sim.checkGenesis(apis)
    */

/*

    assert(sim.checkPeersHealthy(apis))
    sim.logger.info("Peer validation passed")
*/


    // Thread.sleep(30*1000)
   // sim.triggerRandom(apis)



  }

}
