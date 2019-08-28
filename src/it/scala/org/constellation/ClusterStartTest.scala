package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.crypto.KeyUtils
import org.constellation.util.Simulation
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor

class ClusterStartTest extends TestKit(ActorSystem("ClusterTest")) with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  // For fixing some old bug, revisit later if necessary
  KeyUtils.makeKeyPair()

  "Cluster integration" should "ping a cluster, check health, go through genesis flow" in {

    val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()

    val apis = ComputeTestUtil.createApisFromIpFile(ignoreIPs)

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

    Simulation
      .run(apis, addPeerRequests, attemptSetExternalIP = true, useRegistrationFlow = true, useStartFlowOnly = true)
  }

}
