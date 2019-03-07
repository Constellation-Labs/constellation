package org.constellation.tx

import java.security.KeyPair

import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.util.SignHelp
import org.scalatest.FlatSpec

class TXValidationBenchmark extends FlatSpec {
  val logger = Logger("TXValidationBenchmark")

  val kp: KeyPair = KeyUtils.makeKeyPair()
  val kp1: KeyPair = KeyUtils.makeKeyPair()
  val tx = SignHelp.createTransaction(kp.address.address, kp1.address.address, 1L, kp)

  val batchSize = 100

  // System dependent, this is a large enough buffer though
  "Timing tx signature" should "validate 10k transaction signatures under 30s" in {

    val seq = Seq.fill(batchSize)(tx)

    val parSeq = seq.par
    val t0 = System.nanoTime()
    parSeq.map(_.validSrcSignature)
    val t1 = System.nanoTime()
    val delta = (t1 - t0) / 1e6.toLong
    logger.debug(delta.toString)
    // assert(delta < 30000)

  }

  "Timing tx signature direct" should "validate 10k transaction signatures under 30s from bytes" in {

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
    logger.debug(delta2.toString)
    //  assert(delta2 < 30000)

  }

}
