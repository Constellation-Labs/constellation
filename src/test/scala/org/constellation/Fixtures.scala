package org.constellation

import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.constellation.blockchain.{SignTransaction, Transaction}

/**
  * Created by Wyatt on 1/19/18.
  */
object Fixtures {
  implicit val formats = DefaultFormats
  val tx = Transaction(Array.emptyByteArray, "", 0L, "", "" , 1L, "")
  val signTx = SignTransaction("", "", "" , 1L, "")

  def jsonToString[T](obj: T): String = write(obj)

}
