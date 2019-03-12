package org.constellation.crypto

import java.security.KeyPair

import constellation._
import org.constellation.DAO

trait SimpleWalletLike {


  // TODO: Not this.
  @volatile var wallet: Seq[KeyPair] = Seq.fill(10){ KeyUtils.makeKeyPair()}

  // For generating additional keyPairs, maybe make this just regular API call instead.

  def walletPair: KeyPair = {
    val pair = KeyUtils.makeKeyPair()
    wallet :+= pair
    pair
  }

  def addresses: Seq[String] = wallet.map { _.address }

  def addressToKeyPair: Map[String, KeyPair] =
    wallet.map { w =>
      w.address -> w
    }.toMap

}
