package org.constellation.api

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import cats.effect.IO
import org.constellation.crypto.KeyUtils
import org.constellation.domain.configuration.NodeConfig
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeState
import org.constellation.domain.schema.Id
import org.constellation.util.Metrics
import org.constellation.{API, DAO}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor

class ConfigEndpointsApiTest
    extends FreeSpec
    with ScalatestRouteTest
    with Matchers
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats {

  implicit val actorSystem: ActorSystem = system

  var dao: DAO = _
  var api: API = _

  before {
    dao = prepareDao()
    api = new API()(system, Timeout(10, TimeUnit.SECONDS), dao)
  }

  "SimulateTimeout Endpoints" - {
    "should allow to enable simulating timeouts" in {
      Post("/timeout") ~> api.configEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.enableSimulateEndpointTimeout().wasCalled(once)
      }
    }
    "should allow to disable simulating timeouts" in {
      Delete("/timeout") ~> api.configEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.disableSimulateEndpointTimeout().wasCalled(once)
      }
    }
  }

  "RandomTransaction Endpoints" - {
    "should allow to enable random transactions" in {
      Post("/random") ~> api.configEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.enableRandomTransactions().wasCalled(once)
      }
    }
    "should allow to disable random transactions" in {
      Delete("/random") ~> api.configEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.disableRandomTransactions().wasCalled(once)
      }
    }
  }

  "CheckpointFormation Endpoints" - {
    "should allow to enable checkpoint formation" in {
      Post("/checkpointFormation") ~> api.configEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.enableCheckpointFormation().wasCalled(once)
      }
    }
    "should allow to disable checkpoint formation" in {
      Delete("/checkpointFormation") ~> api.configEndpoints ~> check {
        status.isSuccess() shouldEqual true
        dao.disableCheckpointFormation().wasCalled(once)
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
