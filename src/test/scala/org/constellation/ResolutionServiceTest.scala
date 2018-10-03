package org.constellation

import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestActor, TestProbe}
import constellation.signedObservationEdge
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.ResolutionService
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, PeerData}
import org.constellation.util.APIClient
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContextExecutor

class ResolutionServiceTest extends FlatSpec {
  implicit val system: ActorSystem = ActorSystem("ResolutionServiceTest")
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
        sender ! Some(CheckpointCacheData(parentCb, true))
        TestActor.KeepRunning
      case _ =>
        sender ! Some(CheckpointCacheData(parentCb, true))
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


  def getSignedObservationEdge(trx: Transaction) = {
    val ced = CheckpointEdgeData(Seq(tx.edge.signedObservationEdge.signatureBatch.hash))
    val oe = ObservationEdge(
      TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tx.baseHash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(ced.hash, EdgeHashType.CheckpointDataHash))
    )
    val soe: SignedObservationEdge = signedObservationEdge(oe)(keyPair)
    soe
  }

  val soe = getSignedObservationEdge(tx)
  val parentCb = Fixtures.createCheckpointBlock(Seq.fill(3)(tx), Seq.fill(2)(soe))(keyPair)
  val baseHash = soe.hash

  val bogusTx = dummyTx(data, 2L)
  val bogusSoe = getSignedObservationEdge(bogusTx)
  val bogusCb = Fixtures.createCheckpointBlock(Seq.fill(3)(bogusTx), Seq.fill(2)(bogusSoe))(keyPair)

  "CheckpointBlocks with resolved parents" should "have isAhead = false " in {
    val isBranch = ResolutionService.findRoot(mockData, parentCb)
    isBranch.foreach { resolutionStatus =>
      assert(!resolutionStatus.isAhead)
    }
  }

  "CheckpointBlocks with unresolved parents" should "have isAhead = true" in {
    val isBranch = ResolutionService.findRoot(mockData, bogusCb)
    isBranch.foreach { resolutionStatus =>
      assert(resolutionStatus.isAhead)
    }
  }

  "CheckpointBlocks that are invalid " should "have isAhead = false " in {
    val res = ResolutionService.resolveCheckpoint(mockData, parentCb)
    res.foreach { resolutionStatus =>
      assert(!data.snapshotRelativeTips.contains(parentCb))
    }
  }

  "CheckpointBlocks that are ahead" should "query the signers" in {
    val msg = APIBroadcast({ apiClient =>
      apiClient.get("edge/" + bogusCb.baseHash)
    }, peerSubset = bogusCb.signatures.map {
      _.toId
    }.toSet)
    val res = ResolutionService.resolveCheckpoint(mockData, bogusCb)
    res.foreach { resolutionStatus =>
      assert(resolutionStatus.isAhead)
      peerManager.expectMsg(msg)
    }
  }
}
