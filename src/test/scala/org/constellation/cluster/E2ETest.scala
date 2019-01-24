package org.constellation.cluster

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import org.constellation.consensus.StoredSnapshot
import org.constellation.util.{Simulation, TestNode}
import org.constellation.{ConstellationNode, HostPort}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

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
                  seedHosts: Seq[HostPort] = Seq(),
                  portOffset: Int = 0
                ): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(randomizePorts = randomizePorts, portOffset = portOffset, seedHosts = seedHosts)
  }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)


  val totalNumNodes = 3

  private val n1 = createNode(randomizePorts = false)

  //private val address1 = n1.getInetSocketAddress

  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes-1)(i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i*2)+2)) // seedHosts = Seq(address1)

  private val apis = nodes.map{_.getAPIClient()}

  private val addPeerRequests = nodes.map{_.getAddPeerRequest}

  private val sim = new Simulation()

  private val initialAPIs = apis

  "E2E Run" should "demonstrate full flow" in {

    println("API Ports: " + apis.map{_.apiPort})

    assert(sim.run(initialAPIs, addPeerRequests, snapshotCount = 5))

    val downloadNode = createNode(seedHosts = Seq(HostPort("localhost", 9001)), randomizePorts = false, portOffset = 50)

    val downloadAPI = downloadNode.getAPIClient()
    println(s"DownloadNode API Port: ${downloadAPI.apiPort}")
    assert(sim.checkReady(Seq(downloadAPI)))

    Thread.sleep(20*1000)

    val allNodes = nodes :+ downloadNode

    val allAPIs = allNodes.map{_.getAPIClient()} //apis :+ downloadAPI

    // Thread.sleep(1000*1000)

    // Stop transactions
    sim.triggerRandom(allAPIs)

    sim.logger.info("Stopping transactions to run parity check")

    Thread.sleep(30000)

    // TODO: Change assertions to check several times instead of just waiting ^ with sleep
    // Follow pattern in Simulation.await examples
    assert(allAPIs.map{_.metrics("checkpointAccepted")}.distinct.size == 1)
    assert(allAPIs.map{_.metrics("transactionAccepted")}.distinct.size == 1)


    val storedSnapshots = allAPIs.map{_.simpleDownload()}

    val snaps = storedSnapshots.toSet
      .map{x : Seq[StoredSnapshot] =>
        x.map{_.checkpointCache.flatMap{_.checkpointBlock}}.toSet
      }

    assert(
      snaps.size == 1
    )

  }



}
