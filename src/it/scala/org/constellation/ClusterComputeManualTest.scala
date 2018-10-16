package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import constellation._
import org.constellation.util.{APIClient, Simulation}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor


class ClusterComputeManualTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

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


    val apis = ips.map{ ip =>
      new APIClient(ip, 9000)
    }

    val peerAPIs = ips.map{ip =>
      new APIClient(ip, 9001)
    }

    val sim = new Simulation()

    sim.awaitHealthy(apis)

    println(apis.map{
      _.postSync(
        "config/update",
        ProcessingConfig(maxWidth = 10, minCheckpointFormationThreshold = 10, minCBSignatureThreshold = 3)
      )
    })

    sim.run(apis = apis, peerApis = peerAPIs, attemptSetExternalIP = true)


    Thread.sleep(30*1000)
   // sim.triggerRandom(apis)



  }

}
