package org.constellation.schema.v2.signature

import java.security.KeyPair

import org.constellation.schema.v2.edge.{ObservationEdge, SignedObservationEdge}
import org.constellation.schema.v2.{signHashWithKey, signature}

import org.constellation.schema.v2._

trait SignHelpExt {

  def hashSign(hash: String, keyPair: KeyPair): HashSignature =
    signature.HashSignature(
      signHashWithKey(hash, keyPair.getPrivate),
      keyPair.getPublic.toId
    )

  def hashSignBatchZeroTyped(productHash: Signable, keyPair: KeyPair): SignatureBatch = {
    val hash = productHash.hash
    SignatureBatch(hash, Seq(hashSign(hash, keyPair)))
  }

  def signedObservationEdge(oe: ObservationEdge)(implicit kp: KeyPair): SignedObservationEdge =
    SignedObservationEdge(hashSignBatchZeroTyped(oe, kp))

}

object SignHelp extends SignHelpExt
