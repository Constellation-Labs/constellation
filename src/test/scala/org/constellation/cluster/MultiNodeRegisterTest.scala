package org.constellation.cluster

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import org.constellation.ConstellationNode
import org.constellation.p2p.PeerRegistrationRequest
import org.constellation.util.TestNode
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.Try

class MultiNodeRegisterTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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
  }

  def createNode(randomizePorts: Boolean = true, seedHosts: Seq[InetSocketAddress] = Seq()): ConstellationNode = {
    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(seedHosts = seedHosts, randomizePorts = randomizePorts)
  }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

  "E2E Multiple Nodes Register Test" should "add register peers to each other successfully" in {

    val totalNumNodes = 3

    val n1 = createNode(randomizePorts = false)

    val addr = n1.getInetSocketAddress

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(createNode())

    nodes.foreach { node => println(node.getIPData) }

    nodes.foreach { n =>
      assert(n.ipManager.listKnownIPs.isEmpty)
    }

    nodes.combinations(2).foreach { case Seq(n,m) =>
      def register(a: ConstellationNode, b: ConstellationNode): Unit = {
        val ipData = a.getIPData
        val peerRegistrationRequest =
          PeerRegistrationRequest(
            ipData.canonicalHostName,
            ipData.port,
            a.configKeyPair.getPublic.toString
          )
        a.getAPIClientForNode(b).postSync("register", peerRegistrationRequest)
      }
      register(n,m)
      register(m,n)
    }

    Thread.sleep(1000)

    // TODO: Assert actual values of knownIPs list
    nodes.foreach { n => assert(n.ipManager.listKnownIPs.size == 2)}

    assert(true)
  }

}
