package org.constellation.primitives

import java.security.KeyPair

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._
import org.constellation.util.SignatureBatch

case class SetNodeKeyPair(keyPair: KeyPair)
case class SignRequest(data: String)
case class BatchSignRequest(data: SignatureBatch)
case class BatchSignRequestZero(data: TypedProductHash)

import constellation._

class KeyManager(var keyPair: KeyPair = null, memPoolManager: ActorRef, metricsManager: ActorRef) extends Actor {

  def address: String = keyPair.address.address

  def updateSignedMetrics(): Unit = {
    metricsManager ! IncrementMetric("signaturesPerformed")
  }

  override def receive: Receive = {

    case SetNodeKeyPair(kp) => keyPair = kp

    case BatchSignRequestZero(data) =>
      sender() ! hashSignBatchZeroTyped(data, keyPair)
      updateSignedMetrics()

    case BatchSignRequest(data) =>
      sender() ! data.plus(keyPair)
      updateSignedMetrics()

    case SignRequest(data) =>
      sender() ! hashSign(data, keyPair)
      updateSignedMetrics()

    case s: SendToAddress =>
      val txData = TransactionData(address, s.dst, s.amountActual)
      val sig = hashSignBatchZeroTyped(txData, keyPair)
      memPoolManager ! ResolvedTX(TX(sig), txData)
      updateSignedMetrics()

  }
}
