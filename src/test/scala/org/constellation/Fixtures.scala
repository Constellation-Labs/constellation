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
  val publicKey: PublicKey = tempKey.getPublic

}
