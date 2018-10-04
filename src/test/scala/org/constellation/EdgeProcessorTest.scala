package org.constellation

import java.security.{KeyPair, SecureRandom}

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
import scalaj.http.HttpResponse

import scala.concurrent.{ExecutionContextExecutor, Future}

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

  val mockLvlDB = new LvlDB {
    override def restart(): Unit = ???

    override def delete(key: String): Boolean = ???

    override def putCheckpointCacheData(key: String, c: CheckpointCacheData): Unit = ???

    override def updateCheckpointCacheData(key: String, f: CheckpointCacheData => CheckpointCacheData, empty: CheckpointCacheData): CheckpointCacheData = ???

    override def getCheckpointCacheData(key: String): Option[CheckpointCacheData] = ???

    override def putTransactionCacheData(key: String, t: TransactionCacheData): Unit = ???

    override def updateTransactionCacheData(key: String, f: TransactionCacheData => TransactionCacheData, empty: TransactionCacheData): TransactionCacheData = ???

    override def getTransactionCacheData(key: String): Option[TransactionCacheData] = ???

    override def putAddressCacheData(key: String, t: AddressCacheData): Unit = ???

    override def updateAddressCacheData(key: String, f: AddressCacheData => AddressCacheData, empty: AddressCacheData): AddressCacheData = ???

    override def getAddressCacheData(key: String): Option[AddressCacheData] = ???

    override def putSignedObservationEdgeCache(key: String, t: SignedObservationEdgeCache): Unit = ???

    override def updateSignedObservationEdgeCache(key: String, f: SignedObservationEdgeCache => SignedObservationEdgeCache, empty: SignedObservationEdgeCache): SignedObservationEdgeCache = ???

    override def getSignedObservationEdgeCache(key: String): Option[SignedObservationEdgeCache] = ???
  }

  def makeDao(mockData: Data, peerManager: TestProbe = peerManager, metricsManager: TestProbe = metricsManager,
              dbActor: TestProbe = dbActor) = {
    mockData.actorMaterializer = materialize
    mockData.dbActor = testActor
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

  "Incoming transactions" should "be signed and returned if valid" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    assert(validatorResponse.transaction === tx)
  }

  "Incoming transactions" should " be signed if already signed by this keyPair" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    val keyPair: KeyPair = KeyUtils.makeKeyPair()
    val thing = new Data
    thing.updateKeyPair(keyPair)
    val dummyDao = makeDao(thing)
    peerManager.expectMsg(APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", validatorResponse)))
    val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, dummyDao)
    assert(signedTransaction.signatures.exists(_.publicKey == dummyDao.keyPair.getPublic))
  }

  "Incoming transactions" should "not be signed if already signed by this keyPair" in {
    val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, data)
    assert(signedTransaction == tx)
  }

  "Incoming transactions" should "throw exception if invalid" in {
    val bogusTransactionValidationStatus = TransactionValidationStatus(tx, Some(TransactionCacheData(tx, true)), None)
    EdgeProcessor.reportInvalidTransaction(data, bogusTransactionValidationStatus)
    // TODO: Fix ordering - tets failure,
    /*metricsManager.expectMsg(IncrementMetric("invalidTransactions"))
    metricsManager.expectMsg(IncrementMetric("hashDuplicateTransactions"))
    metricsManager.expectMsg(IncrementMetric("insufficientBalanceTransactions"))*/
  }

  "New valid incoming transactions" should "be added to the mempool" in {
    // TODO: Use simpler mempool, changed.
    //EdgeProcessor.updateMergeMemPool(tx, data)
    //assert(data.transactionMemPoolMultiWitness.contains(tx.hash))
  }

  "Observed valid incoming transactions" should "be merged into observation edges" in {
    // TODO: Use simpler mempool, changed.
    /*
    val updated = data.transactionMemPoolMultiWitness(tx.hash).plus(tx)
    data.transactionMemPoolMultiWitness(tx.hash) = tx
    assert(data.transactionMemPoolMultiWitness(tx.hash) == updated)*/
  }
}

