package org.constellation

import java.security.{KeyPair, PublicKey}

import akka.actor.ActorRef
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.constellation.blockchain.{BlockData, Block, Transaction}
import org.constellation.wallet.KeyUtils
import org.constellation.wallet.KeyUtils.{KeyPairSerializer, PrivateKeySerializer, PublicKeySerializer}

import scala.collection.mutable

/**
  * Created by Wyatt on 1/19/18.
  */
object Fixtures {

  val tempKey: KeyPair = KeyUtils.makeKeyPair()
  val tx = Transaction(0L, tempKey.getPublic, tempKey.getPublic, 1L)
  // val signTx = SignTransaction("", "", "" , 1L, "")
  val checkpointBlock = Block("hashPointer", 0L, "signature", mutable.HashMap.empty[ActorRef, Option[BlockData]], 0L)
  val genesisBlock = Block("genesisBlock", 0L, "signature", mutable.HashMap.empty[ActorRef, Option[BlockData]], 0L)
  val publicKey: PublicKey = tempKey.getPublic

}


