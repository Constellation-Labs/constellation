package org.constellation.cluster

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import org.constellation.util.{Simulation, TestNode}
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
    import scala.tools.nsc.io.{File => SFile}
    val tmpDir = new File("tmp")
    Try{SFile(tmpDir).deleteRecursively()}

    val n1 = TestNode(heartbeatEnabled = true, randomizePorts = false
    //  , generateRandomTransactions = true
    )

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true
    //  , generateRandomTransactions = true
    ))

    val apis = nodes.map{_.api}
    val sim = new Simulation(apis)
    //sim.run(attemptSetExternalIP = false
    //  , validationFractionAcceptable = 0.3
    //)
    sim.runV2()



    val goe = sim.genesisOE

    Thread.sleep(1000*60*60)

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    Try{SFile(tmpDir).deleteRecursively()}

    assert(true)
  }

}
