package org.constellation.cluster


import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.app.AppNode
import org.constellation.p2p.PeerToPeer.{AddPeer, GetPeers, Peers}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContextExecutor}

class MultiAppTest extends FlatSpec with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  "Multiple Apps" should "create and run multiple nodes within the same JVM" in {

    (1 to 3).foreach { _ =>
      AppNode.apply()
    }
    // Need for verifications as in SingleAppTest, i.e. health checks

  }

  "Multiple Connected Apps" should "create and run multiple nodes and talk to each other" in {

    val seedHostNode = AppNode.apply()

    implicit val timeout: Timeout = seedHostNode.timeout

    val seedHostPath = seedHostNode.blockChainActor.path.toSerializationFormat

    val nodes = (1 to 3).map { _ =>
      AppNode.apply()
    }

    nodes.foreach{ n =>
      n.blockChainActor ! AddPeer(seedHostPath)
    }

    // TODO : Remove this, I tried an ask above (which should return a response, but it timed out
    // after 5 seconds. Only modifying this to make other fixes, needs revisiting.
    Thread.sleep(500)

    nodes.foreach{ n =>
      import scala.concurrent.duration._
      val response = Await.result(n.blockChainActor ? GetPeers, 5.seconds).asInstanceOf[Peers]
      assert(response.peers.nonEmpty)
    }

    // Need to verify nodes are healthy

  }

  override def afterAll() {
    system.terminate()
  }

}
