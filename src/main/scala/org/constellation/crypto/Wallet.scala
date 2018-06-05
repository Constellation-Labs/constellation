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

  val peerToPeerActor : ActorRef
  val data: Data
  import data._

  implicit val timeout: Timeout



  // TODO: Not this.
  @volatile var wallet : Seq[KeyPair] = Seq()

  def walletPair: KeyPair = {
    val pair = constellation.makeKeyPair()
    wallet :+= pair
    pair
  }

  def addresses: Seq[String] = wallet.map{_.address.address}

  def addressToKeyPair: Map[String, KeyPair] = wallet.map{ w => w.address.address -> w}.toMap

  def walletAddressInfo: Map[String, TX] = {
    wallet.map{_.address.address}.flatMap{k => db.getAs[TX](k).map{k -> _}}.toMap
  }

  def outputBalances: Seq[Address] = walletAddressInfo.flatMap{
    case (k,v) =>
      v.output(k)
  }.toSeq

  def selfIdBalance: Option[Long] = memPoolUTXO.get(selfAddress.address)

  def utxoBalance: Map[String, Long] = {
    validUTXO.filter{case (x,y) => addresses.contains(x)}
  }.toMap

  def handleSendRequest(s: SendToAddress): StandardRoute = {

    if (s.useNodeKey) {
      val srcAddress = selfAddress
      val dstAddress = s.address

      val tx = TX(
        TXData(
          Seq(srcAddress), dstAddress, s.amountActual
        ).signed()(keyPair)
      )

      sentTX :+= tx

      //   logger.info(s"SendToAddress RPC Transaction: ${tx.pretty}")

      peerToPeerActor ! tx

      complete(tx.json)

    } else {
      complete(StatusCodes.BadRequest)
    }

    //, StatusCodes.Accepted)

    // Do full UTXO later, simplifying it for now to use 1 address.
    /*          val ut = utxoBalance
              val (addressWithSufficientBalance, prvBalance) = ut.filter{_._2 > s.amountActual}.head
              val txAssociated = walletAddressInfo(addressWithSufficientBalance)
              val addressMeta = txAssociated.output(addressWithSufficientBalance).get
              val ukp = addressToKeyPair(addressWithSufficientBalance)
              val remainder = walletPair


              val remainderBalance = prvBalance - s.amountActual

              println(s"Send To Address $prvBalance $addressWithSufficientBalance $addressMeta $txAssociated")

              val genHash = if (txAssociated.tx.data.genesisTXHash.isEmpty && txAssociated.tx.data.isGenesis)
                Some(txAssociated.hash) else txAssociated.tx.data.genesisTXHash

              // TODO: Fix hashes.
              val txD = TXData(
                Seq(addressMeta.copy(
                  balance = 0L,
                  txHashPool = addressMeta.txHashPool :+ txAssociated.hash
                )),
                s.address.copy(
                  balance = s.amountActual
                ),
                s.amountActual,
                remainder = Some(remainder.address.copy(
                  balance = remainderBalance
                )),
                srcAccount = Some(id.id),
                dstAccount = s.account,
                genesisTXHash = genHash
              )

              val tx = TX(txD.multiSigned()(Seq(ukp, keyPair)))
    */

    //   complete(StatusCodes.OK)
  }


}
