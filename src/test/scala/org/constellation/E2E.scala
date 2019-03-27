package org.constellation
import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.constellation.util.{APIClient, Simulation, TestNode}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, ExecutionContextExecutorService}
import scala.util.Try

trait E2E
  extends AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with StrictLogging {

  implicit val system: ActorSystem = ActorSystem("ConstellationTestNode")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
//  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

  val tmpDir = "tmp"

  override def beforeAll(): Unit = {
    // Cleanup DBs
    //Try{File(tmpDir).delete()}
    Try { File(tmpDir).createDirectories() }

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
                ): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(
      randomizePorts = randomizePorts,
      portOffset = portOffset,
      seedHosts = seedHosts,
      isGenesisNode = isGenesisNode,
      isLightNode = isLightNode
    )
  }

}
