package org.constellation

import java.security.{KeyPair, PublicKey}

import akka.actor.ActorRef
import org.constellation.primitives.Block
import org.constellation.primitives.Transaction
import org.constellation.wallet.KeyUtils

import scala.collection.mutable

object Fixtures {

  val tempKey: KeyPair = KeyUtils.makeKeyPair()
  val tx = Transaction(0L, tempKey.getPublic, tempKey.getPublic, 1L)
  // val signTx = SignTransaction("", "", "" , 1L, "")
//  val checkpointBlock = primitives.Block.Block("hashPointer", 0L, "signature", mutable.HashMap.empty[ActorRef, Option[Block]], 0L)
//  val genesisBlock = primitives.Block.Block("genesisBlock", 0L, "signature", mutable.HashMap.empty[ActorRef, Option[Block]], 0L)
  val publicKey: PublicKey = tempKey.getPublic

}
