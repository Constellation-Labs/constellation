package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import better.files._
import org.constellation.util.{Simulation, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.ExecutionContextExecutor



class MultiNodeDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
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
    File(tmpDir).delete(true)

    val n1 = TestNode(heartbeatEnabled = true, randomizePorts = false
    //  , generateRandomTransactions = true
    )

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true
    //  , generateRandomTransactions = true
    ))

    val apis = nodes.map{_.api}
    val sim = new Simulation(apis)
    sim.run(attemptSetExternalIP = false
      , validationFractionAcceptable = 0.3
    )

   // Thread.sleep(1000*60*60)

    // Cleanup DBs
    File(tmpDir).delete(true)
    assert(true)
  }

}
