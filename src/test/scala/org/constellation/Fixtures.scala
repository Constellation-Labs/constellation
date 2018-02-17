package org.constellation

import akka.actor.ActorRef
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.constellation.blockchain.{BlockData, CheckpointBlock, SignTransaction, Transaction}

import scala.collection.mutable

/**
  * Created by Wyatt on 1/19/18.
  */
object Fixtures {
  implicit val formats = DefaultFormats
  val tx = Transaction(Array.emptyByteArray, "", 0L, "", "" , 1L, "")
  val signTx = SignTransaction("", "", "" , 1L, "")
  val checkpointBlock = CheckpointBlock("hashPointer", 0L, "signature", mutable.HashMap.empty[ActorRef, Option[BlockData]], 0L)
  val genesisBlock = CheckpointBlock("genesisBlock", 0L, "signature", mutable.HashMap.empty[ActorRef, Option[BlockData]], 0L)
  val publicKey = "publicKey"

  def jsonToString[T](obj: T): String = write(obj)

}
