package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import constellation.makeKeyPair
import org.constellation.util.{APIClient, Simulation}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor
import org.constellation.primitives.Schema.NodeState

class ClusterSingleDownloadJoinTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val kp = makeKeyPair()

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {


    import better.files._

    val ips = file"hosts.txt".lines.toSeq

    println(ips)

    val ips2 = file"hosts2.txt".lines.toSeq


    val apis = ips.map{ ip =>
      new APIClient().setConnection(ip, 9000)
    }

    val peerAPIs = ips.map{ip =>
      new APIClient().setConnection(ip, 9001)
    }


    val apis2 = ips2.map{ ip =>
      new APIClient().setConnection(ip, 9000)
    }

    val peerAPIs2 = ips2.map{ip =>
      new APIClient().setConnection(ip, 9001)
    }

    val sim = new Simulation()

    sim.setIdLocal(apis)
    sim.setIdLocal(apis2)

    sim.awaitHealthy(apis2)

    apis2.map{_.postSync(
      "config/update",
      ProcessingConfig(maxWidth = 10, minCheckpointFormationThreshold = 10, minCBSignatureThreshold = 3)
    )}


    apis.foreach {
      a =>
        sim.addPeer(apis2, AddPeerRequest(a.hostName, a.udpPort, 9001, a.id)).foreach{println}
    }

    apis2.foreach{
      a =>
        sim.addPeer(apis, AddPeerRequest(a.hostName, a.udpPort, 9001, a.id, NodeState.DownloadInProgress)).foreach{println}
    }

    apis2.foreach{
      _.postEmpty("download/start")
    }


    Thread.sleep(30*1000)


  }

}
