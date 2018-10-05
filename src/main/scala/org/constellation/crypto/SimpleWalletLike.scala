package org.constellation.crypto

import java.security.KeyPair

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import org.constellation.primitives.Schema._
import constellation._
import akka.util.Timeout
import org.constellation.DAO

trait SimpleWalletLike {

  val dao: DAO
  import dao._

  // TODO: Not this.
  @volatile var wallet : Seq[KeyPair] = Seq()

  // For generating additional keyPairs, maybe make this just regular API call instead.
  def walletPair: KeyPair = {
    val pair = constellation.makeKeyPair()
    wallet :+= pair
    pair
  }

  def addresses: Seq[String] = wallet.map{_.address.address}

  def addressToKeyPair: Map[String, KeyPair] = wallet.map{ w => w.address.address -> w}.toMap

}
