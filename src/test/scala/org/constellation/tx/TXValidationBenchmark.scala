package org.constellation.tx

import com.typesafe.scalalogging.Logger
import org.constellation.Fixtures._
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.domain.transaction.TransactionValidator
import org.scalatest.FlatSpec

class TXValidationBenchmark extends FlatSpec {
  val logger = Logger("TXValidationBenchmark")

  val batchSize = 500

  // System dependent, this is a large enough buffer though
  "Timing tx signature" should s"validate $batchSize transaction signatures under 30s" in {

    val seq = Seq.fill(batchSize)(tx)

    val parSeq = seq.par
    val t0 = System.nanoTime()
    parSeq.map(TransactionValidator.validateSourceSignature)
    val t1 = System.nanoTime()
    val delta = (t1 - t0) / 1e6.toLong
    logger.debug(s"Validated $batchSize transactions in $delta ms")
    assert(delta < 30000)

  }

  "Timing tx signature direct" should s"validate $batchSize transaction signatures under 30s from bytes" in {

    val batch = tx.edge.signedObservationEdge.signatureBatch
    val sig = batch.signatures.head
    val pkey = sig.publicKey

    val hashBytes = batch.hash.getBytes()
    val signatureBytes = hex2bytes(sig.signature)
    assert(KeyUtils.verifySignature(hashBytes, signatureBytes)(pkey))

    val seq2 = Seq.fill(batchSize)(0).par
    val t0a = System.nanoTime()
    seq2.map(_ => KeyUtils.verifySignature(hashBytes, signatureBytes)(pkey))
    val t1a = System.nanoTime()
    val delta2 = (t1a - t0a) / 1e6.toLong
    logger.debug(s"Validated $batchSize transaction signatures from bytes in $delta2 ms")
    assert(delta2 < 30000)

  }

}
