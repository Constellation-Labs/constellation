package org.constellation.crypto

import java.security.KeyPair

import constellation._
import org.constellation.DAO

/** @todo See comments in body. */
trait SimpleWalletLike {

  val dao: DAO // not used here // tmp comment

  // TODO: Not this.
  @volatile var wallet: Seq[KeyPair] = Seq()

  // For generating additional keyPairs, maybe make this just regular API call instead.

  /** @todo Documentation */
  def walletPair: KeyPair = {
    val pair = KeyUtils.makeKeyPair()
    wallet :+= pair
    pair
  }

  /** @todo Documentation */
  def addresses: Seq[String] = wallet.map {
    _.address.address
  }

  /** @todo Documentation */
  def addressToKeyPair: Map[String, KeyPair] = wallet.map { w => w.address.address -> w }.toMap

}
