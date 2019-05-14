package org.constellation.p2p
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestProbe
import com.softwaremill.sttp.Response
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.DAO
import org.constellation.consensus.{FinishedCheckpoint, FinishedCheckpointAck, FinishedCheckpointResponse}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.IPManager
import org.constellation.primitives.Schema.{CheckpointCache, Id, NodeState}
import org.constellation.util.Metrics
import org.json4s.native
import org.json4s.native.Serialization
import org.mockito.Mockito
import org.mockito.integrations.scalatest.IdiomaticMockitoFixture
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class PeerAPITest
    extends WordSpec
    with Matchers
    with ScalatestRouteTest
    with IdiomaticMockitoFixture
    with Json4sSupport {

  implicit val serialization: Serialization.type = native.Serialization
  implicit val s: ActorSystem = system
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val dao: DAO = prepareDao()

  val peerAPI: PeerAPI = new PeerAPI(new IPManager, TestProbe().ref)

  "The PeerAPI" should {
    /*
        Unfortunately ScalatestRouteTest instansiate it's own class of PeerAPI thus we can't spy on it
     */
    "return acknowledge message on finishing checkpoint and reply with callback" ignore {
      val reply = "http://originator:9001/peer-api/finished/checkpoint/reply"
      val fakeResp = Future.successful(mock[Response[Unit]])
      Mockito
        .doReturn(fakeResp,fakeResp)
        .when(peerAPI)
        .makeCallback(*, *)

      val req = FinishedCheckpoint(CheckpointCache(None), Set.empty)

      Post("/finished/checkpoint", req) ~> addHeader("ReplyTo", reply) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[FinishedCheckpointAck] shouldEqual FinishedCheckpointAck(true)
      }

      //      requires mockito 1.4.x and migrating all IdiomaticMockitoFixtures
      //      peerAPI.makeCallback(*, *) wasCalled (once within 2.seconds)
    }

    "return acknowledge message on finishing checkpoint and make no reply with callback" in {
      val req = FinishedCheckpoint(CheckpointCache(None), Set.empty)
      Post("/finished/checkpoint", req) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[FinishedCheckpointAck] shouldEqual FinishedCheckpointAck(true)
      }
    }
    "handle reply message" in {
      Post("/finished/reply", FinishedCheckpointResponse(true)) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

  private def prepareDao(): DAO = {
    implicit val dao: DAO = mock[DAO]
    dao.finishedExecutionContext shouldReturn executionContext
    dao.edgeExecutionContext shouldReturn executionContext
    val id = Id("node1")
    dao.id shouldReturn id
    val keyPair = KeyUtils.makeKeyPair()
    dao.keyPair shouldReturn keyPair
    dao.nodeState shouldReturn NodeState.Ready
    val metrics = new Metrics
    dao.metrics shouldReturn metrics
    dao
  }
}
