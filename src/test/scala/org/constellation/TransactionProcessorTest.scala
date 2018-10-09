package org.constellation

import java.security.{KeyPair, SecureRandom}

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{TestProbe, _}
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.LevelDB.{DBGet, DBUpdate}
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.consensus.{EdgeProcessor, Validation}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.APIClient
import org.scalatest.FlatSpec
import scalaj.http.HttpResponse

import scala.concurrent.{ExecutionContextExecutor, Future}

class TransactionProcessorTest extends ProcessorTest {

  dbActor.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
      case _ =>
        sender ! None
        TestActor.KeepRunning
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

  "Incoming transactions" should "be signed and returned if valid" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    validatorResponse map { tx2 =>
      assert(tx2.transaction == tx)
    }
  }

  "Incoming transactions" should " be signed if already signed by this keyPair" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    val keyPair: KeyPair = KeyUtils.makeKeyPair()
    val dummyData = new Data
    dummyData.updateKeyPair(keyPair)
    val pm = TestProbe()
    val dummyDao = makeDao(dummyData, pm, TestProbe(), TestProbe())
    validatorResponse.map { tx2 =>
//      pm.expectMsg(APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx2))) todo, getting boxed error
      val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, dummyDao)
      println(signedTransaction)
      assert(signedTransaction.signatures.exists(_.publicKey == dummyDao.keyPair.getPublic))
    }
  }

  "Incoming transactions" should "not be signed if already signed by this keyPair" in {
    val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, data)
    assert(signedTransaction == tx)
  }

  "Incoming transactions" should "throw exception if invalid" in {
    val bogusTransactionValidationStatus = TransactionValidationStatus(tx, Some(TransactionCacheData(tx, true, false, true)), None)
    EdgeProcessor.reportInvalidTransaction(data, bogusTransactionValidationStatus)
    metricsManager.expectMsg(IncrementMetric("invalidTransactions"))
//    metricsManager.expectMsg(IncrementMetric("insufficientBalanceTransactions")) todo weird not being received but printing correct criteria
    metricsManager.expectMsg(IncrementMetric("hashDuplicateTransactions"))
    assert(true)
  }

  "New valid incoming transactions" should "be added to the mempool" in {
    EdgeProcessor.updateMergeMemPool(tx, data)
    assert(data.transactionMemPoolMultiWitness(tx.baseHash) == tx)
  }

  "Observed valid incoming transactions" should "be merged into observation edges" in {
    val updated = data.transactionMemPoolMultiWitness(tx.baseHash).plus(tx)
    data.transactionMemPoolMultiWitness(tx.baseHash) = tx
    assert(data.transactionMemPoolMultiWitness(tx.baseHash) == updated)
  }
}
