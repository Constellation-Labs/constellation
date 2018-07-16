package org.constellation.cluster

import java.io.File
import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem

import scala.concurrent.duration._
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import constellation._
import org.constellation.ConstellationNode
import org.constellation.primitives.Schema._
import org.constellation.util.{Simulation, TestNode}
import org.scalatest._

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

class MultiNodeDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
  with Matchers with WordSpecLike with BeforeAndAfterEach {

  override def afterEach() {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "Multiple Constellation Nodes" when {

    "running consensus" should {

      val totalNumNodes = 3

      val n1 = TestNode(heartbeatEnabled = true, randomizePorts = true)

      val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true))

      val apis = nodes.map{_.api}

      val sim = new Simulation()

      var validTxs = Set[Transaction]()

      val connectedNodes = sim.connectNodes(false, true, apis)

      "handle random transactions with a stable set of nodes" in {

        val start = System.currentTimeMillis()

        validTxs = sim.sendRandomTransactions(20, apis)

        assert(sim.validateRun(validTxs, 1.0, apis))

        val end = System.currentTimeMillis()

        println(s"Completion time seconds: ${(end-start) / 1000}")

        assert(true)
      }

      "handle adding a node to an existing cluster" in {
        val start = System.currentTimeMillis()

        val newNode = TestNode(heartbeatEnabled = true).api

        val updatedNodes = apis :+ newNode

        sim.connectNodes(false, false, updatedNodes)

        assert(sim.validateRun(validTxs, 1.0, updatedNodes))

        assert(true)

        val end = System.currentTimeMillis()

        println(s"Completion time seconds: ${(end-start) / 1000}")
      }

    }

  }

}
