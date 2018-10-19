package org.constellation

import java.security.KeyPair

import akka.testkit.TestProbe
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.consensus.{EdgeProcessor, Validation}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.scalatest.FlatSpec

class TransactionProcessorTest extends FlatSpec with ProcessorTest {
  (data.dbActor.getTransactionCacheData _).when(txHash).returns(Some(TransactionCacheData(tx, false)))
  (data.dbActor.getTransactionCacheData _).when(invalidSpendHash).returns(Some(TransactionCacheData(tx, true)))
  (data.dbActor.getAddressCacheData _).when(srcHash).returns(Some(AddressCacheData(100000000000000000L, 100000000000000000L, None)))

  "Incoming transactions" should "be signed and returned if valid" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
      assert(validatorResponse.transaction == tx)
  }

  "Incoming transactions" should " be signed if already signed by this keyPair" in {
    val validatorResponse = Validation.validateTransaction(data.dbActor,tx)
    val keyPair: KeyPair = KeyUtils.makeKeyPair()
    val dummyData = new Data
    dummyData.updateKeyPair(keyPair)
    val pm = TestProbe()
    val dummyDao = makeDao(dummyData, pm, TestProbe(), TestProbe())
    val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, dummyDao)
    pm.expectMsg(_: APIBroadcast.type)
    assert(signedTransaction.signatures.exists(_.publicKey == dummyDao.keyPair.getPublic))
  }

  "Incoming transactions" should "not be signed if already signed by this keyPair" in {
    val signedTransaction = EdgeProcessor.updateWithSelfSignatureEmit(tx, data)
    assert(signedTransaction == tx)
  }

  "Incoming transactions" should "throw exception if invalid" in {
    val bogusTransactionValidationStatus = TransactionValidationStatus(tx, Some(TransactionCacheData(tx, true, false, true)), None)
    EdgeProcessor.reportInvalidTransaction(data, bogusTransactionValidationStatus)
    metricsManager.expectMsg(IncrementMetric("invalidTransactions"))
    metricsManager.expectMsg(IncrementMetric("hashDuplicateTransactions"))
    metricsManager.expectMsg(IncrementMetric("insufficientBalanceTransactions"))
  }

  "New valid incoming transactions" should "be added to the mempool" in {
    EdgeProcessor.updateMergeMemPool(tx, data)
    assert(data.transactionMemPoolMultiWitness(tx.baseHash) == tx)
  }

  "Observed valid incoming transactions" should "be merged into observation edges" in {
    EdgeProcessor.updateMergeMemPool(tx, data)
    val updated: Transaction = data.transactionMemPoolMultiWitness(tx.baseHash).plus(tx)
    assert(data.transactionMemPoolMultiWitness(tx.baseHash) == updated)
  }
}
