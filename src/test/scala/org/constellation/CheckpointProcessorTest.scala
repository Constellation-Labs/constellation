package org.constellation

import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActor, TestProbe}
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.EdgeProcessor
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.{APIBroadcast, IncrementMetric, PeerData}
import org.constellation.primitives.Schema.{AddressCacheData, Id, Transaction, TransactionCacheData}
import org.constellation.util.APIClient
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContextExecutor

class CheckpointProcessorTest extends FlatSpec {
  implicit val system: ActorSystem = ActorSystem("CheckpointProcessorTest")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val keyPair: KeyPair = KeyUtils.makeKeyPair()
  val peerData = PeerData(addPeerRequest, getAPIClient("", 1))
  val peerManager = TestProbe()
  val metricsManager = TestProbe()
  val dbActor = TestProbe()
  dbActor.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
      case DBGet(`srcHash`) =>
        sender ! Some(AddressCacheData(100000000000000000L, 100000000000000000L, None))
        TestActor.KeepRunning

      case DBGet(`txHash`) =>
        sender ! Some(TransactionCacheData(tx, false))
        TestActor.KeepRunning

      case DBGet(`invalidSpendHash`) =>
        sender ! Some(TransactionCacheData(tx, true))
        TestActor.KeepRunning
    }
  })

  val mockData = new Data
  mockData.updateKeyPair(keyPair)

  def makeDao(mockData: Data, peerManager: TestProbe = peerManager, metricsManager: TestProbe = metricsManager,
              dbActor: TestProbe = dbActor) = {
    mockData.actorMaterializer = materialize
    mockData.dbActor = dbActor.testActor
    mockData.metricsManager = metricsManager.testActor
    mockData.peerManager = peerManager.testActor
    mockData
  }
  val data = makeDao(mockData)
  val tx: Transaction = dummyTx(data)
  val invalidTx = dummyTx(data, -1L)
  val srcHash = tx.src.hash
  val txHash = tx.hash
  val invalidSpendHash = invalidTx.hash
  val randomPeer: (Id, PeerData) = (id, peerData)

  def getAPIClient(hostName: String, httpPort: Int) = {
    val api = new APIClient().setConnection(host = hostName, port = httpPort)
    api.id = id
    api.udpPort = 16180
    api
  }

  val bogusTxValidStatus = TransactionValidationStatus(tx, None, None)
  val cb = Fixtures.dummyCheckpointBlock(data)

  "Incoming CheckpointBlocks" should "be signed and processed if new" in {
    EdgeProcessor.handleCheckpoint(cb, data)
  }

  "Previously observed incoming CheckpointBlocks" should "be handled internally" in {
    metricsManager.expectMsg(IncrementMetric("dupCheckpointReceived"))
  }

  "CheckpointBlocks valid by state" should "return true" in {
    val validatedCheckpointBlock = EdgeProcessor.validateCheckpointBlock(data, cb)
    validatedCheckpointBlock.foreach(response => assert(response))  }

  "CheckpointBlocks valid by ancestry" should "return true" in {
    EdgeProcessor.validByAncestors(Seq(), cb)
  }


  "CheckpointBlocks invalid by state but valid by ancestor" should "be signed and returned if valid" in {
    val validatedCheckpointBlock = EdgeProcessor.validateCheckpointBlock(data, cb)
    validatedCheckpointBlock.foreach(response => assert(response))
  }

  "CheckpointBlocks valid according to current state" should "be signed and returned if valid" in {
    val cbPrime = if (!cb.signatures.exists(_.publicKey == data.keyPair.getPublic)) {
      val signedCb = cb.plus(data.keyPair)
      peerManager.expectMsg(APIBroadcast(_.put(s"checkpoint/${cb.baseHash}", signedCb)))
    }
  }


  "hashToSignedObservationEdgeCache" should "return SignedObservationEdgeCache" in {
    val res = data.hashToSignedObservationEdgeCache(cb.baseHash)
    res.foreach(response => assert(response.isDefined))

  }

  "hashToCheckpointCacheData" should "return CheckpointCacheData" in {
    val res = data.hashToCheckpointCacheData(cb.baseHash)
    res.foreach(response => assert(response.isDefined))
  }
}
