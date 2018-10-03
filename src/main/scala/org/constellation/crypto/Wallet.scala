package org.constellation.crypto

import java.security.KeyPair

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import org.constellation.primitives.Schema._
import constellation._
import akka.util.Timeout
import org.constellation.Data

trait Wallet {

  val dao: Data
  import dao._

  // TODO: Not this.
  @volatile var wallet : Seq[KeyPair] = Seq()

  def walletPair: KeyPair = {
    val pair = constellation.makeKeyPair()
    wallet :+= pair
    pair
  }

  def addresses: Seq[String] = wallet.map{_.address.address}

  def addressToKeyPair: Map[String, KeyPair] = wallet.map{ w => w.address.address -> w}.toMap

/*
  def walletAddressInfo: Map[String, TX] = {
    wallet.map{_.address.address}.flatMap{k =>(k).map{k -> _}}.toMap
  }
*/

/*
  def outputBalances: Seq[AddressMetaData] = walletAddressInfo.flatMap{
    case (k,v) =>
      v.output(k)
  }.toSeq
*/

  def selfIdBalance: Option[Long] = validLedger.get(selfAddress.address)

  def utxoBalance: Map[String, Long] = {
    validLedger.filter{case (x,y) => addresses.contains(x)}
  }.toMap

}
