package org.constellation.api

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import cats.effect.IO
import org.constellation.crypto.KeyUtils
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.{Id, NodeState}
import org.constellation.util.Metrics
import org.constellation.{API, DAO, NodeConfig}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor

class TimeoutEndpointApiTest
    extends FreeSpec
    with ScalatestRouteTest
    with Matchers
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats {

  implicit val actorSystem: ActorSystem = system
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var dao: DAO = _
  var api: API = _

  before {
    dao = prepareDao()
    api = new API()(system, Timeout(10, TimeUnit.SECONDS), dao)
  }

  "Timeout Endpoints" - {
    "should get current value simulateEndpointTimeout variable" in {
      Get("/timeout") ~> api.getEndpoints ~> check {
        status.isSuccess() shouldEqual true
        responseAs[String] shouldBe dao.simulateEndpointTimeout.toString
      }
    }

    "should change value simulateEndpointTimeout variable" in {
      Post("/timeout") ~> api.postEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.toggleSimulateEndpointTimeout().wasCalled(once)
      }
    }
  }

  private def prepareDao(): DAO = {
    val dao: DAO = mock[DAO]

    dao.nodeConfig shouldReturn NodeConfig()
    dao.id shouldReturn Id("node1")
    dao.keyPair shouldReturn KeyUtils.makeKeyPair()
    dao.cluster shouldReturn mock[Cluster[IO]]
    dao.cluster.getNodeState shouldReturn IO.pure(NodeState.Ready)

    val metrics = new Metrics(1)(dao)
    dao.metrics shouldReturn metrics

    dao
  }
}
