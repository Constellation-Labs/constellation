package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import better.files.File
import org.constellation.util.{Simulation, TestNode}

import scala.concurrent.duration._
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

class MultiNodeDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
  with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val tmpDir = "tmp"

  override def beforeEach(): Unit = {
    // Cleanup DBs
    Try{File(tmpDir).delete()}
  }

  override def afterEach() {
    // Cleanup DBs
    File(tmpDir).delete()
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

  "E2E Multiple Nodes DAG" should "add peers and build DAG with transactions" in {

    val totalNumNodes = 3

    val n1 = TestNode(heartbeatEnabled = true, randomizePorts = false)

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true))

    val apis = nodes.map{_.getAPIClient()}

    val peerApis = nodes.map{ node => {
      val n = node.getAPIClient()
      n.apiPort = node.peerHttpPort
      n
    }}

    val sim = new Simulation()

    sim.run(apis = apis, peerApis = peerApis)

    Thread.sleep(5000*60*60)
/*

    var txs = 3

    while (txs > 0) {
      sim.sendRandomTransaction(apis)
      txs = txs - 1
    }
*/

    /*
    val probe = TestProbe()

    within(1 hour) {
      probe.expectMsg(1 hour, "a message")
    }
    */

    // TODO: add random transactions and verifications
 //   val checkpointTips = sim.getCheckpointTips(apis)

    assert(true)
  }

}
