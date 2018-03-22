package org.constellation.utils

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.ConstellationNode
import org.constellation.wallet.KeyUtils

import scala.concurrent.ExecutionContextExecutor

object TestNode {

  def apply(seedHosts: Option[Seq[String]] = None, keyPair: KeyPair = KeyUtils.makeKeyPair())(
    implicit system: ActorSystem,
    materialize: ActorMaterializer,
    executionContext: ExecutionContextExecutor
  ): ConstellationNode = {
    val randomPort = scala.util.Random.nextInt(50000) + 5000
    new ConstellationNode(keyPair, seedHosts, "0.0.0.0", randomPort)
  }

}
