package org.constellation.p2p

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import better.files.File
import cats.effect.IO
import com.softwaremill.sttp.Response
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.consensus.{FinishedCheckpoint, FinishedCheckpointResponse}
import org.constellation.crypto.KeyUtils
import org.constellation.domain.configuration.NodeConfig
import org.constellation.primitives.Schema.{CheckpointCache, Height, NodeState}
import org.constellation.domain.schema.Id
import org.constellation.primitives.{IPManager, Transaction, TransactionCacheData, TransactionGossip}
import org.constellation.storage.VerificationStatus.{SnapshotCorrect, SnapshotHeightAbove, SnapshotInvalid}
import org.constellation.storage._
import org.constellation.storage.transactions.TransactionGossiping
import org.constellation.util.{APIClient, Metrics}
import org.constellation.{DAO, Fixtures}
import org.json4s.native
import org.json4s.native.Serialization
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class PeerAPITest
    extends FreeSpec
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

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger
  implicit val cs = IO.contextShift(executionContext)
  implicit val timer = IO.timer(executionContext)

  var dao: DAO = _
  var peerAPI: PeerAPI = _

  before {
    dao = prepareDao()
    dao.snapshotBroadcastService shouldReturn mock[SnapshotBroadcastService[IO]]
    peerAPI = new PeerAPI(IPManager[IO])(system, 10.seconds, dao)
  }

  "The PeerAPI" - {
    /*
        Unfortunately ScalatestRouteTest instansiate it's own class of PeerAPI thus we can't spy on it
     */
    "return accepted on finishing checkpoint and reply with callback when header is defined".ignore {
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
      dao.snapshotService.getNextHeightInterval shouldReturnF 2
      val req = FinishedCheckpoint(CheckpointCache(None, 0, Some(Height(1, 1))), Set.empty)
      Post("/finished/checkpoint", req) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }
    "should handle reply message" in {
      Post("/finished/reply", FinishedCheckpointResponse(true)) ~> peerAPI.postEndpoints ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "should return snapshot bytes when stored snapshot exist" in {
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

    "should return snapshot not found when snapshot does not exist" in {
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

    "snapshot/verify endpoint" - {
      val request = SnapshotCreated("snap1", 2)
      val path = "/snapshot/verify"

      "should return correct state" in {
        val recent = List(
          RecentSnapshot("snap2", 4),
          RecentSnapshot("snap1", 2)
        )

        dao.snapshotBroadcastService.getRecentSnapshots shouldReturnF recent

        Post(path, request) ~> peerAPI.postEndpoints ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[SnapshotVerification] shouldBe SnapshotVerification(dao.id, SnapshotCorrect, recent)
        }
      }

      "should return invalid state when there no recent snapshots given" in {
        dao.snapshotBroadcastService.getRecentSnapshots shouldReturnF List.empty

        Post(path, request) ~> peerAPI.postEndpoints ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[SnapshotVerification] shouldBe SnapshotVerification(dao.id, SnapshotInvalid, List.empty)
        }
      }

      "should return height above state when given height is above current" in {
        val recent = List(RecentSnapshot("snap2", 1))
        dao.snapshotBroadcastService.getRecentSnapshots shouldReturnF recent

        Post(path, request) ~> peerAPI.postEndpoints ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[SnapshotVerification] shouldBe SnapshotVerification(dao.id, SnapshotHeightAbove, recent)
        }
      }
    }

    "snapshot/recent endpoint" - {
      val path = "/snapshot/recent"

      "should return list of recent snapshots" in {
        dao.snapshotBroadcastService.getRecentSnapshots shouldReturnF List(
          RecentSnapshot("snap2", 4),
          RecentSnapshot("snap1", 2)
        )
        Get(path) ~> peerAPI.commonEndpoints ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[List[RecentSnapshot]] shouldBe List(RecentSnapshot("snap2", 4), RecentSnapshot("snap1", 2))
        }
      }

      "should return empty list" in {
        dao.snapshotBroadcastService.getRecentSnapshots shouldReturnF List.empty

        Get(path) ~> peerAPI.commonEndpoints ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[List[RecentSnapshot]] shouldBe List.empty
        }
      }
    }

    "mixedEndpoints" - {
      "PUT transaction" - {

        "should observe received transaction" in {
          dao.transactionGossiping shouldReturn mock[TransactionGossiping[IO]]
          dao.transactionGossiping.observe(*) shouldReturnF mock[TransactionCacheData]
          dao.transactionGossiping.selectPeers(*)(scala.util.Random) shouldReturnF Set()
          dao.peerInfo shouldReturnF Map()

          val a = KeyUtils.makeKeyPair()
          val b = KeyUtils.makeKeyPair()

          val tx = createTransaction(a.address, b.address, 5L, a)

          Put(s"/transaction", TransactionGossip(tx)) ~> peerAPI.mixedEndpoints ~> check {
            dao.transactionGossiping.observe(*).was(called)
          }
        }

        "should broadcast transaction to others".ignore {
          val a = KeyUtils.makeKeyPair()
          val b = KeyUtils.makeKeyPair()

          val tx = createTransaction(a.address, b.address, 5L, a)
          val tcd = mock[TransactionCacheData]

          val id = Fixtures.id2
          val peerData = mock[PeerData]
          peerData.client shouldReturn mock[APIClient]
          peerData.client.putAsync(*, *, *)(*) shouldReturnF mock[Response[String]]

          dao.transactionGossiping shouldReturn mock[TransactionGossiping[IO]]
          dao.transactionGossiping.observe(*) shouldReturnF tcd
          dao.transactionGossiping.selectPeers(tcd)(scala.util.Random) shouldReturnF Set(id)
          dao.peerInfo shouldReturnF Map(id -> peerData)

          Put(s"/transaction", TransactionGossip(tx)) ~> peerAPI.mixedEndpoints ~> check {
            peerData.client.putAsync(*, *, *)(*).was(called)
          }
        }

        "should return StatusCodes.OK" in {
          dao.transactionGossiping shouldReturn mock[TransactionGossiping[IO]]
          dao.transactionGossiping.observe(*) shouldReturnF mock[TransactionCacheData]
          dao.transactionGossiping.selectPeers(*)(scala.util.Random) shouldReturnF Set()
          dao.peerInfo shouldReturnF Map()

          val a = KeyUtils.makeKeyPair()
          val b = KeyUtils.makeKeyPair()

          val tx = createTransaction(a.address, b.address, 5L, a)

          Put(s"/transaction", TransactionGossip(tx)) ~> peerAPI.mixedEndpoints ~> check {
            status shouldEqual StatusCodes.OK
          }
        }
      }

      "POST transactions" - {
        "should return list of transactions if one of two transaction exists" in {
          val a = KeyUtils.makeKeyPair()
          val b = KeyUtils.makeKeyPair()
          val tx = createTransaction(a.address, b.address, 5L, a)

          dao.transactionService shouldReturn mock[TransactionService[IO]]
          dao.transactionService.lookup("hash1") shouldReturnF Some(new TransactionCacheData(tx))
          dao.transactionService.lookup("none") shouldReturnF None
          dao.peerInfo shouldReturnF Map()

          val hashes = List("hash1", "none")

          Post(s"/batch/transactions", hashes) ~> peerAPI.batchEndpoints ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[List[(String, TransactionCacheData)]].size shouldEqual 1
          }
        }

        "should return empty list if transactions are not exist" in {
          dao.transactionService shouldReturn mock[TransactionService[IO]]
          dao.transactionService.lookup(*) shouldReturnF None
          dao.peerInfo shouldReturnF Map()

          val hashes = List("none1", "none2")

          Post(s"/batch/transactions", hashes) ~> peerAPI.batchEndpoints ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[List[(String, TransactionCacheData)]].size shouldEqual 0
          }
        }
      }
    }
  }

  private def prepareDao(): DAO = {
    val dao: DAO = mock[DAO]

    dao.nodeConfig shouldReturn NodeConfig()

    val id = Id("node1")
    dao.id shouldReturn id

    val keyPair = KeyUtils.makeKeyPair()
    dao.keyPair shouldReturn keyPair

    dao.cluster shouldReturn mock[Cluster[IO]]
    dao.cluster.getNodeState shouldReturn IO.pure(NodeState.Ready)

    val metrics = new Metrics(1)(dao)
    dao.metrics shouldReturn metrics

    val ipManager = IPManager[IO]()
    val cluster = Cluster[IO](() => metrics, ipManager, dao)
    dao.cluster shouldReturn cluster
    dao.cluster.setNodeState(NodeState.Ready).unsafeRunSync
    dao.peerInfo shouldReturnF Map()

    dao.snapshotService shouldReturn mock[SnapshotService[IO]]
    dao.checkpointAcceptanceService shouldReturn mock[CheckpointAcceptanceService[IO]]
    dao.checkpointAcceptanceService.accept(any[FinishedCheckpoint]) shouldReturn IO({
      Thread.sleep(2000)
    })
    dao
  }
}
