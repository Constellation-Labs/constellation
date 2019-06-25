package org.constellation.p2p

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestProbe
import better.files.File
import cats.effect.IO
import com.softwaremill.sttp.Response
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.DAO
import org.constellation.consensus.{FinishedCheckpoint, FinishedCheckpointResponse}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.{ConcurrentTipService, IPManager}
import org.constellation.primitives.Schema.{CheckpointCache, Id, NodeState}
import org.constellation.storage.{
  AddressService,
  CheckpointService,
  MessageService,
  SnapshotService,
  TransactionService
}
import org.constellation.util.Metrics
import org.json4s.native
import org.json4s.native.Serialization
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class PeerAPITest
    extends WordSpec
    with Matchers
    with ScalatestRouteTest
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with Json4sSupport
    with BeforeAndAfter {

  implicit val serialization: Serialization.type = native.Serialization
  implicit val s: ActorSystem = system
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val contextShift = IO.contextShift(executionContext)
  implicit val timer = IO.timer(executionContext)

  var dao: DAO = _
  var peerAPI: PeerAPI = _

  before {
    dao = prepareDao()
    peerAPI = new PeerAPI(new IPManager, TestProbe().ref)(system, 10.seconds, dao)
  }

  "The PeerAPI" should {
    /*
        Unfortunately ScalatestRouteTest instansiate it's own class of PeerAPI thus we can't spy on it
     */
    "return accepted message on finishing checkpoint and reply with callback" ignore {
      val reply = "http://originator:9001/peer-api/finished/checkpoint/reply"
      val fakeResp = Future.successful(mock[Response[Unit]])
      Mockito
        .doReturn(fakeResp, fakeResp)
        .when(peerAPI)
        .makeCallback(*, *)

      val req = FinishedCheckpoint(CheckpointCache(None), Set.empty)

      Post("/finished/checkpoint", req) ~> addHeader("ReplyTo", reply) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.Accepted
      }

      //      requires mockito 1.4.x and migrating all IdiomaticMockitos
      //      peerAPI.makeCallback(*, *) wasCalled (once within 2.seconds)
    }

    "return accepted on finishing checkpoint and make no reply with callback" in {
      val req = FinishedCheckpoint(CheckpointCache(None), Set.empty)
      Post("/finished/checkpoint", req) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }
    "handle reply message" in {
      Post("/finished/reply", FinishedCheckpointResponse(true)) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "return snapshot bytes when stored snapshot exist" in {
      dao.snapshotService shouldReturn mock[SnapshotService[IO]]
      dao.snapshotService.exists(*) shouldReturnF true

      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { file =>
          val snapshotHash = file.name
          dao.snapshotPath shouldReturn dir

          Get(s"/storedSnapshot/$snapshotHash") ~> peerAPI.commonEndpoints ~> check {
            status shouldEqual StatusCodes.OK
          }
        }
      }

    }

    "return snapshot not found when snapshot does not exist" in {
      dao.snapshotService shouldReturn mock[SnapshotService[IO]]
      dao.snapshotService.exists(*) shouldReturnF false

      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { _ =>
          val snapshotHash = "other_file"
          dao.snapshotPath shouldReturn dir

          Get(s"/storedSnapshot/$snapshotHash") ~> peerAPI.commonEndpoints ~> check {
            status shouldEqual StatusCodes.NotFound
          }
        }
      }

    }
  }

  private def prepareDao(): DAO = {
    val dao: DAO = mock[DAO]

    val id = Id("node1")
    dao.id shouldReturn id

    val keyPair = KeyUtils.makeKeyPair()
    dao.keyPair shouldReturn keyPair

    dao.nodeState shouldReturn NodeState.Ready

    val metrics = new Metrics(1)(dao)
    dao.metrics shouldReturn metrics

    dao.checkpointService shouldReturn mock[CheckpointService[IO]]
    dao.checkpointService.accept(any[FinishedCheckpoint])(dao) shouldReturn IO({
      Thread.sleep(2000)
    })
    dao
  }
}
