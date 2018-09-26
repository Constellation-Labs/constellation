package org.constellation

import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestProbe, _}
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.LevelDB.DBGet
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.consensus.{EdgeProcessor, Validation}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.APIClient
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContextExecutor

class EdgeProcessorTest extends FlatSpec {
  implicit val system: ActorSystem = ActorSystem("TransactionProcessorTest")
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
        sender ! Some(AddressCacheData(100000000000000000L, None))
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
    val api = APIClient(host = hostName, port = httpPort, udpPort = 16180)
    api.id = id
    api
  }

  "Incoming transactions" should "be signed and returned if valid" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    validatorResponse foreach { tx2 =>
      assert(tx2.transaction == tx)
    }
  }

  "Incoming transactions" should " be signed if already signed by this keyPair" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    val keyPair: KeyPair = KeyUtils.makeKeyPair()
    val thing = new Data
    thing.updateKeyPair(keyPair)
    val dummyDao = makeDao(thing)
    validatorResponse.foreach { tx2 =>
      peerManager.expectMsg(APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx2)))
      val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, dummyDao)
      assert(signedTransaction.signatures.exists(_.publicKey == dummyDao.keyPair.getPublic))
    }
  }

  "Incoming transactions" should "not be signed if already signed by this keyPair" in {
    val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, data)
    assert(signedTransaction == tx)
  }

  "Incoming transactions" should "throw exception if invalid" in {
    val bogusTransactionValidationStatus = TransactionValidationStatus(tx, Some(TransactionCacheData(tx, true)), None)
    EdgeProcessor.reportInvalidTransaction(data, bogusTransactionValidationStatus)
    metricsManager.expectMsg(IncrementMetric("invalidTransactions"))
    metricsManager.expectMsg(IncrementMetric("hashDuplicateTransactions"))
    metricsManager.expectMsg(IncrementMetric("insufficientBalanceTransactions"))
  }

  "New valid incoming transactions" should "be added to the mempool" in {
    EdgeProcessor.updateMergeMemPool(tx, data)
    assert(data.transactionMemPool.contains(tx.hash))
  }


  "Observed valid incoming transactions" should "be merged into observation edges" in {
    val updated = data.transactionMemPool(tx.hash).plus(tx)
    data.transactionMemPool(tx.hash) = tx
    assert(data.transactionMemPool(tx.hash) == updated)
  }
}

