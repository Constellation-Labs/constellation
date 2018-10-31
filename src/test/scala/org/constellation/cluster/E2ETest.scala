package org.constellation.cluster

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import org.constellation.ConstellationNode
import org.constellation.primitives.Schema.CheckpointBlock
import org.constellation.util.{Simulation, TestNode}
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
    //Try{File(tmpDir).delete()}
    //Try{new java.io.File(tmpDir).mkdirs()}

  }

  override def afterAll() {
    // Cleanup DBs
    TestNode.clearNodes()
    system.terminate()
    Try{File(tmpDir).delete()}
  }

  def createNode(
                  randomizePorts: Boolean = true,
                  seedHosts: Seq[InetSocketAddress] = Seq(),
                  portOffset: Int = 0
                ): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(randomizePorts = randomizePorts, portOffset = portOffset)
  }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)


  val totalNumNodes = 3

  private val n1 = createNode(randomizePorts = false)

  //private val address1 = n1.getInetSocketAddress

  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes-1)(i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i*2)+2)) // seedHosts = Seq(address1)

  private val apis = nodes.map{_.getAPIClient()}

  private val addPeerRequests = nodes.map{_.getAddPeerRequest}

  private val sim = new Simulation()

  private val downloadIndex = 3

  private val initialAPIs = apis.slice(0, downloadIndex)

  //private val downloadAPIs = apis.slice(downloadIndex, totalNumNodes + 1)

  "E2E Run" should "demonstrate full flow" in {

    println("API Ports: " + apis.map{_.apiPort})

    assert(sim.run(initialAPIs, addPeerRequests.slice(0, downloadIndex)))

    //Thread.sleep(1000*1000)

    // Stop transactions
    sim.triggerRandom(apis)

    sim.logger.info("Stopping transactions to run parity check")

    Thread.sleep(30000)

    // TODO: Change assertions to check several times instead of just waiting ^ with sleep
    // Follow pattern in Simulation.await examples
    assert(apis.map{_.metrics("checkpointAccepted")}.distinct.size == 1)
    assert(apis.map{_.metrics("transactionAccepted")}.distinct.size == 1)

    val blockHashes = apis.map{_.simpleDownload()}

    assert(blockHashes.map{_.size}.distinct.size == 1)
    assert(blockHashes.map{_.toSet}.distinct.size == 1)

    val downloadedBlocks = apis.map{ a =>
      blockHashes.head.distinct.map{ h =>
        a.getBlocking[Option[CheckpointBlock]]("checkpoint/" + h)
      }
    }

    assert(downloadedBlocks.forall(_.forall(_.nonEmpty)))
    assert(downloadedBlocks.map(_.map{_.get}.toSet).distinct.size == 1)

    /*
    snapshotBlocksDistinct.flatMap{ cb =>
      val cbA =
      println("MISSING CHECKPOINT")
      cbA
    }
*/
/*


    assert(blocks.map{_.size}.distinct.size == 1)
    assert(blocks.forall{b => b.size == b.distinct.size})
    assert(blocks.map{_.toSet}.distinct.size == 1)
*/

    // TODO: Fix download test flakiness. Works sometimes

/*

    assert(sim.checkHealthy(downloadAPIs))

    val downloadNodePeerRequests = addPeerRequests.slice(downloadIndex, totalNumNodes + 1)
      .map{_.copy(nodeStatus = NodeState.DownloadInProgress)}

    sim.addPeersFromRequest(downloadAPIs, downloadNodePeerRequests)

    downloadNodePeerRequests.foreach{ apr =>
      sim.addPeer(initialAPIs, apr)
    }

    addPeerRequests.slice(0, downloadIndex).foreach{ apr =>
      sim.addPeer(downloadAPIs, apr)
    }

    assert(sim.checkPeersHealthy(apis))

    downloadAPIs.foreach{ a2 =>
      a2.postEmpty("download/start")
      a2.postEmpty("random")
    }

    assert(sim.checkReady(downloadAPIs))

    //Thread.sleep(5000)

    // Stop transactions
    sim.triggerRandom(apis)

    Thread.sleep(15000)
    assert(apis.map{_.metrics("checkpointAccepted")}.distinct.size == 1)
    assert(apis.map{_.metrics("transactionAccepted")}.distinct.size == 1)

    val blocks = apis.map{_.simpleDownload()}

    assert(blocks.map{_.size}.distinct.size == 1)
    assert(blocks.forall{b => b.size == b.distinct.size})
    assert(blocks.map{_.toSet}.distinct.size == 1)
*/

  }

}
