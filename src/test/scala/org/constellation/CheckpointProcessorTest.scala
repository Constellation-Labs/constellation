package org.constellation

import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActor, TestProbe}
import constellation.signedObservationEdge
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.EdgeProcessor
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.{APIBroadcast, IncrementMetric, PeerData}
import org.constellation.primitives.Schema._
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
      case DBGet(`baseHash`) =>
        sender ! Some(CheckpointCacheData(cb, true))
        TestActor.KeepRunning
      case _ =>
        sender ! None
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
  val ced = CheckpointEdgeData(Seq(tx.edge.signedObservationEdge.signatureBatch.hash))

  val oe = ObservationEdge(
    TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
    TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
    data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
  )

  val soe = signedObservationEdge(oe)(keyPair)
  val cb = Fixtures.createCheckpointBlock(Seq.fill(3)(tx), Seq.fill(2)(soe))(keyPair)
  val baseHash = cb.baseHash

  "Incoming CheckpointBlocks" should "be signed and processed if new" in {
    EdgeProcessor.handleCheckpoint(cb, data)
    metricsManager.expectMsg(IncrementMetric("checkpointMessages"))
  }

  "Previously observed CheckpointBlocks" should "indicate to metricsManager" in {
    metricsManager.expectMsg(IncrementMetric("dupCheckpointReceived"))
  }

  "Invalid CheckpointBlocks" should "return false" in {
    val validatedCheckpointBlock = EdgeProcessor.validateCheckpointBlock(data, cb)
    validatedCheckpointBlock.foreach(response => assert(!response))
  }


  "CheckpointBlocks invalid by ancestry" should "return false" in {
    assert(!EdgeProcessor.validByTransactionAncestors(Seq(), cb))
  }

  "CheckpointBlocks invalid by state" should "return false" in {
    val validatedCheckpointBlock = EdgeProcessor.validateCheckpointBlock(data, cb)
    validatedCheckpointBlock.foreach(response => assert(!response))
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
