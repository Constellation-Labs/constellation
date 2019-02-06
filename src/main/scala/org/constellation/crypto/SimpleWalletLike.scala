package org.constellation.crypto

import java.security.KeyPair
import org.constellation.DAO

import constellation._

trait SimpleWalletLike {

  val dao: DAO

  // TODO: Not this.
  @volatile var wallet: Seq[KeyPair] = Seq()

  // For generating additional keyPairs, maybe make this just regular API call instead.

  def walletPair: KeyPair = {
    val pair = KeyUtils.makeKeyPair()
    wallet :+= pair
    pair
  }

  def addresses: Seq[String] = wallet.map { _.address.address }

  def addressToKeyPair: Map[String, KeyPair] =
    wallet.map { w =>
      w.address.address -> w
    }.toMap

}
