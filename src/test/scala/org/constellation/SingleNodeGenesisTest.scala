package org.constellation

import java.util.concurrent.ForkJoinPool
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import better.files.File
import com.typesafe.scalalogging.Logger
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Try

import org.constellation.util.TestNode

class SingleNodeGenesisTest extends FlatSpec with BeforeAndAfterAll {

  val logger = Logger("SingleNodeGenesisTest")

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
    Try { File(tmpDir).delete() }
  }

  def createNode(
                  randomizePorts: Boolean = false,
                  portOffset: Int = 0,
                  isGenesisNode: Boolean = false
                ): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(
      randomizePorts = randomizePorts,
      portOffset = portOffset,
      seedHosts = Seq(),
      isGenesisNode = isGenesisNode
    )
  }

  //"Genesis created"
  ignore should "verify the node has created genesis" in {

    val node = createNode(isGenesisNode = true)
    val api = node.getAPIClient()

    //Thread.sleep(6000*1000)

  }

}