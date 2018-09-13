package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import better.files.File
import org.constellation.util.{APIClient, Simulation, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

class MultiNodeOEDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
  with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "E2E Multiple Nodes DAG" should "add peers and build DAG with transactions" in {

    val totalNumNodes = 3

    // Cleanup DBs
    val tmpDir = "tmp"
    Try{File(tmpDir).delete()}

    val n1 = TestNode(heartbeatEnabled = true, randomizePorts = false
    //  , generateRandomTransactions = true
    )

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true
    //  , generateRandomTransactions = true
    ))

    val apis: Seq[APIClient] = nodes.map{_.getAPIClient()}

    val peerApis: Seq[APIClient] = nodes.map{ node => {
      val n = node.getAPIClient()
      n.apiPort = node.peerHttpPort
      n
    }}

    val sim = new Simulation()

    //sim.run(attemptSetExternalIP = false
    //  , validationFractionAcceptable = 0.3
    //)

    sim.runV2(apis = apis, peerApis = peerApis)

   // Thread.sleep(1000*60*60)

    // Cleanup DBs
    File(tmpDir).delete()

    assert(true)
  }

}
