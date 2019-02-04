package org.constellation.crypto

import java.security.KeyPair

import constellation._
import org.constellation.DAO

/** Documentation. */
trait SimpleWalletLike {

  val dao: DAO

  // TODO: Not this.
  @volatile var wallet : Seq[KeyPair] = Seq()

  // For generating additional keyPairs, maybe make this just regular API call instead.

  /** Documentation. */
  def walletPair: KeyPair = {
    val pair = KeyUtils.makeKeyPair()
    wallet :+= pair
    pair
  }

  /** Documentation. */
  def addresses: Seq[String] = wallet.map{_.address.address}

  /** Documentation. */
  def addressToKeyPair: Map[String, KeyPair] = wallet.map{ w => w.address.address -> w}.toMap

}
