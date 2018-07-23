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
import org.constellation.util.{APIClient, Simulation, TestNode}
import org.scalatest._

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class MultiNodeDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
  with Matchers with WordSpecLike with BeforeAndAfterEach with BeforeAndAfterAll {

  var cluster: TestCluster = _

  override def beforeEach(): Unit = {
    cluster = createInitialCluster()
  }

  override def afterEach() {
    cluster.nodes.foreach(n => n.shutdown())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "Multiple Constellation Nodes" when {

    "running consensus" should {

      "handle random transactions with a stable set of nodes" in {

        // send random transactions
        val validTxs = cluster.sim.sendRandomTransactions(20, cluster.apis)

        // validate consensus on transactions
        assert(cluster.sim.validateRun(validTxs, 1.0, cluster.apis))

        assert(true)
      }

      "handle adding a node to an existing cluster" in {

        // send random transactions
        var validTxs = cluster.sim.sendRandomTransactions(20, cluster.apis)

        // validate consensus on transactions for the initial nodes
        assert(cluster.sim.validateRun(validTxs, 1.0, cluster.apis))

        // create a new node
        val newNode = TestNode(heartbeatEnabled = true).api

        val updatedNodes = cluster.apis :+ newNode

        // add the new node to the cluster
        cluster.sim.connectNodes(false, true, updatedNodes)

        // validate that the new node catches up and comes to consensus
        assert(cluster.sim.validateRun(validTxs, 1.0, updatedNodes))

        // add some more transactions
        validTxs = validTxs.++(cluster.sim.sendRandomTransactions(5, updatedNodes))

        // validate consensus on all of the transactions and nodes
        // TODO: investigate why this one is getting stuck
        assert(cluster.sim.validateRun(validTxs, .9, updatedNodes))

        assert(true)
      }

    }

  }

  case class TestCluster(apis: Seq[APIClient], nodes: Seq[ConstellationNode], sim: Simulation)

  def createInitialCluster(numberOfNodes: Int = 3): TestCluster = {

    val n1 = TestNode(heartbeatEnabled = true)

    val nodes = Seq(n1) ++ Seq.fill(numberOfNodes-1)(TestNode(heartbeatEnabled = true))

    val apis = nodes.map{_.api}
    val sim = new Simulation()

    sim.connectNodes(false, true, apis)

    TestCluster(apis, nodes, sim)
  }

}
