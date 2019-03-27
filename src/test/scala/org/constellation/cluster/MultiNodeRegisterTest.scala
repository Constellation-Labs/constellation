package org.constellation.cluster

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.util.TestNode
import org.constellation.{ConstellationNode, HostPort, ResourceInfo}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, _}

import scala.concurrent.ExecutionContext
import scala.util.Try

class MultiNodeRegisterTest
    extends FunSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  val logger = Logger("MultiNodeRegisterTest")

  val tmpDir = "tmp"

  implicit val system: ActorSystem = ActorSystem("ConstellationTestNode")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def beforeEach(): Unit = {
    // Cleanup DBs
    Try { File(tmpDir).delete() }
  }

  override def afterEach() {
    // Cleanup DBs
    File(tmpDir).delete()
    TestNode.clearNodes()
    system.terminate()
  }

  def createNode(randomizePorts: Boolean = true,
                 seedHosts: Seq[HostPort] = Seq()): ConstellationNode = {
    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(seedHosts = seedHosts, randomizePorts = randomizePorts)
  }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)


  describe("E2E Multiple Nodes Register Test") {
    it("add register peers to each other successfully") {

      val otherNodesSize = 2

      val n1 = createNode(randomizePorts = false)

      val nodes = Seq(n1) ++ Seq.fill(otherNodesSize)(createNode())

      nodes.foreach { n =>
        assert(n.dao.peerInfo.isEmpty)
        assert(n.ipManager.listKnownIPs.isEmpty)
      }

      nodes.combinations(otherNodesSize).foreach {
        case Seq(n, m) =>
          def register(a: ConstellationNode, b: ConstellationNode): Unit = {
            val ipData = a.getIPData
            val peerRegistrationRequest =
              PeerRegistrationRequest(
                ipData.canonicalHostName,
                ipData.port,
                a.dao.keyPair.getPublic.toId,
              ResourceInfo(diskUsableBytes = 1073741824)
            )
          val res = a.getAPIClientForNode(b).postSync("register", peerRegistrationRequest)
          assert(res.isSuccess)
        }
        register(n, m)
        register(m, n)

      }
      Thread.sleep(1000)
      nodes.foreach { n =>
        assert(n.dao.peerInfo.size == otherNodesSize)
      }

    }

  }
}
