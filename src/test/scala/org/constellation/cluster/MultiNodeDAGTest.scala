package org.constellation.cluster

import java.io.File
import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import constellation._
import org.constellation.ConstellationNode
import org.constellation.primitives.Schema._
import org.constellation.util.{Simulation, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Random, Try}


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
    import scala.tools.nsc.io.{File => SFile}
    val tmpDir = new File("tmp")
    Try{SFile(tmpDir).deleteRecursively()}

    val n1 = TestNode(heartbeatEnabled = true, randomizePorts = false)

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true))

    val apis = nodes.map{_.api}
    val sim = new Simulation(apis)
    sim.run(attemptSetExternalIP = false)

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    Try{SFile(tmpDir).deleteRecursively()}

    assert(true)
  }

}
