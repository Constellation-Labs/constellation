package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.constellation.util.{HostPort, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.util.Try

trait E2E extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with StrictLogging {

  implicit val system: ActorSystem = ActorSystem("ConstellationTestNode")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
//  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

  val tmpDir = "/tmp"

  override def beforeAll(): Unit = {
    Try { File(tmpDir).createDirectories() }
    Try { File(tmpDir).delete() }
  }

  override def afterAll() {
    // Cleanup DBs
    TestNode.clearNodes()
    system.terminate()
    Try { File(tmpDir).delete() }
  }

  def createNode(
    randomizePorts: Boolean = true,
    seedHosts: Seq[HostPort] = Seq(),
    portOffset: Int = 0,
    isGenesisNode: Boolean = false,
    isLightNode: Boolean = false
  ): ConstellationNode =
    TestNode(
      randomizePorts = randomizePorts,
      portOffset = portOffset,
      seedHosts = seedHosts,
      isGenesisNode = isGenesisNode,
      isLightNode = isLightNode
    )

}
