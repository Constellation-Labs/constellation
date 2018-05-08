package org.constellation.primitives


import java.security.PublicKey

import org.constellation.util.{ProductHash, Signed}

// This can't be a trait due to serialization issues
object Schema {

  case class Address(address: String)

  case class TX(
                 src: Seq[Address],
                 dst: Address,
                 amount: Long,
                 remainder: Option[Address] = None,
                 keyMap: Seq[Int] = Seq()
               ) extends ProductHash

  case class SignedTX(tx: Signed[TX]) {
    def valid: Boolean = {
      val km = tx.data.keyMap
      val signatureAddresses = if (km.nonEmpty) {
        val store = Array.fill(km.toSet.size)(Seq[PublicKey]())
        km.zipWithIndex.foreach{ case (keyGroupIdx, keyIdx) =>
          store(keyGroupIdx) = store(keyGroupIdx) :+ tx.publicKeys(keyIdx)
        }
        store.map{constellation.pubKeysToAddress}.toSeq
      } else {
        tx.publicKeys.map{constellation.pubKeyToAddress}
      }
      val validInputAddresses = signatureAddresses == tx.data.src
      validInputAddresses && tx.valid
    }
  }

}
