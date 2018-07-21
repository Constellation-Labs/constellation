package org.constellation.primitives

import java.security.KeyPair

import akka.actor.Actor
import org.constellation.primitives.Schema.TypedProductHash
import org.constellation.util.SignatureBatch

case class SetNodeKeyPair(keyPair: KeyPair)
case class SignRequest(data: String)
case class BatchSignRequest(data: SignatureBatch)
case class BatchSignRequestZero(data: TypedProductHash)

import constellation._

class KeyManager(var keyPair: KeyPair = null) extends Actor {

  override def receive: Receive = {

    case SetNodeKeyPair(kp) => keyPair = kp

    case BatchSignRequestZero(data) => sender() ! hashSignBatchZeroTyped(data, keyPair)

    case BatchSignRequest(data) => sender() ! data.plus(keyPair)

    case SignRequest(data) => sender() ! hashSign(data, keyPair)

  }
}
