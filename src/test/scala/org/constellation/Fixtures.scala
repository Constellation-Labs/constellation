package org.constellation

import akka.actor.ActorRef
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.constellation.blockchain.{CheckpointBlock, SignTransaction, Transaction}

import scala.collection.mutable

/**
  * Created by Wyatt on 1/19/18.
  */
object Fixtures {
  implicit val formats = DefaultFormats
  val tx = Transaction(Array.emptyByteArray, "", 0L, "", "" , 1L, "")
  val signTx = SignTransaction("", "", "" , 1L, "")
  val checkpointBlock = CheckpointBlock("hashPointer", 0L, "signature")
  val genesisBlock = CheckpointBlock("genesisBlock", 0L, "signature")
  val publicKey = "publicKey"

  def jsonToString[T](obj: T): String = write(obj)

}
