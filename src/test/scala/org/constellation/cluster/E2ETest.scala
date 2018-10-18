package org.constellation.cluster

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import org.constellation.primitives.Schema.NodeState
import org.constellation.{AddPeerRequest, ConstellationNode, HostPort}
import org.constellation.util.{APIClient, Simulation, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Try

class E2ETest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val tmpDir = "tmp"

  implicit val system: ActorSystem = ActorSystem("ConstellationTestNode")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def beforeAll(): Unit = {
    // Cleanup DBs
    Try{File(tmpDir).delete()}
  }

  override def afterAll() {
    // Cleanup DBs
    Try{File(tmpDir).delete()}
    TestNode.clearNodes()
    system.terminate()
  }

  def createNode(randomizePorts: Boolean = true, seedHosts: Seq[InetSocketAddress] = Seq()): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(randomizePorts = randomizePorts)
  }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)


  val totalNumNodes = 5

  private val n1 = createNode(randomizePorts = false)

  //private val address1 = n1.getInetSocketAddress

  private val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(createNode(seedHosts = Seq())) // seedHosts = Seq(address1)

  private val apis = nodes.map{_.getAPIClient()}

  private val addPeerRequests = nodes.map{_.getAddPeerRequest}

  private val sim = new Simulation()

  private val initialAPIs = apis.slice(0, 3)

  private val downloadAPIs = apis.slice(3, totalNumNodes + 1)

  "E2E Run" should "demonstrate full flow" in {


    assert(sim.run(initialAPIs, addPeerRequests.slice(0,3)))

    assert(sim.checkHealthy(downloadAPIs))

    val downloadNodePeerRequests = addPeerRequests.slice(3, totalNumNodes + 1)
      .map{_.copy(nodeStatus = NodeState.DownloadInProgress)}

    sim.addPeersFromRequest(downloadAPIs, downloadNodePeerRequests)

    downloadNodePeerRequests.foreach{ apr =>
      sim.addPeer(initialAPIs, apr)
    }

    addPeerRequests.slice(0, 3).foreach{ apr =>
      sim.addPeer(downloadAPIs, apr)
    }

    assert(sim.checkPeersHealthy(apis))

    downloadAPIs.foreach{ a2 =>
      a2.postEmpty("download/start")
      a2.postEmpty("random")
    }

    assert(sim.checkReady(downloadAPIs))

  }

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

   // assert(true)

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
