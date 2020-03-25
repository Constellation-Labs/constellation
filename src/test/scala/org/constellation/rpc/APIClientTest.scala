package org.constellation.rpc

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.softwaremill.sttp.{SttpBackend, SttpBackendOptions}
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.keytool.KeyUtils
import org.constellation.schema.Id
import org.constellation.util.{APIClient, TestNode}
import org.constellation.{ConfigUtil, ConstellationExecutionContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class APIClientTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with StrictLogging {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()

  implicit val backend: SttpBackend[Future, Nothing] =
    PrometheusBackend[Future, Nothing](
      OkHttpFutureBackend(
        SttpBackendOptions.connectionTimeout(ConfigUtil.getDurationFromConfig("connection-timeout", 5.seconds))
      )(
        ConstellationExecutionContext.unbounded
      )
    )

  override def afterEach(): Unit =
    TestNode.clearNodes()

  "GET to /id" should "get the current nodes public key id" in {
    val keyPair = KeyUtils.makeKeyPair()
    val appNode = TestNode(Seq(), keyPair)
    val rpc = APIClient(port = appNode.nodeConfig.httpPort)
    val id = rpc.getBlocking[Id]("id")

    assert(keyPair.getPublic.toId == id)
  }

  override def afterAll() {
    system.terminate()
  }

}
