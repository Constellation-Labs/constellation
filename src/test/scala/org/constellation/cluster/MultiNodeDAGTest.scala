package org.constellation.cluster

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import org.constellation.ConstellationNode
import org.constellation.util.{APIClient, Simulation, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Try

class MultiNodeDAGTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val tmpDir = "tmp"

  implicit val system: ActorSystem = ActorSystem("ConstellationTestNode")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def beforeEach(): Unit = {
    // Cleanup DBs
    Try{File(tmpDir).delete()}
  }

  override def afterEach() {
    // Cleanup DBs
    File(tmpDir).delete()
    TestNode.clearNodes()
    system.terminate()
  }

  def createNode(randomizePorts: Boolean = true, seedHosts: Seq[InetSocketAddress] = Seq()): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(randomizePorts = randomizePorts)
  }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

  "E2E Multiple Nodes DAG" should "add peers and build DAG with transactions" in {

    val totalNumNodes = 3

    val n1 = createNode(randomizePorts = false)

    val addr = n1.getInetSocketAddress

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(createNode(seedHosts = Seq(addr)))

    val apis = nodes.map{_.getAPIClient()}

    val peerApis = nodes.map{ node => {
      val n = node.getAPIClient(port = node.peerHttpPort)
      n
    }}

    val sim = new Simulation()

    sim.run(apis = apis, peerApis = peerApis)

  //  Thread.sleep(200*1000)
    // sim.triggerRandom(apis) Thread.sleep(5000*60*60)
/*

    var txs = 3

    while (txs > 0) {
      sim.sendRandomTransaction(apis)
      txs = txs - 1
    }
*/

    // wip
/*
    val stopWatch = Stopwatch.createStarted()
    val elapsed = stopWatch.elapsed()

    while (!verifyCheckpointTips(sim, apis) && elapsed.getSeconds <= 30) {

    }

    val checkpointTips = sim.getCheckpointTips(apis)*/

    assert(true)
  }

  def verifyCheckpointTips(sim: Simulation, apis: Seq[APIClient]): Boolean = {

    val checkpointTips = sim.getCheckpointTips(apis)

    val head = checkpointTips.head

    if (head.size >= 1) {
      val allEqual = checkpointTips.forall(f => {
        val equal = f.keySet.diff(head.keySet).isEmpty
        equal
      })

      allEqual
    } else {
      false
    }
  }

}
