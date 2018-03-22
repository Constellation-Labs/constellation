package org.constellation.app

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.ConstellationNode
import org.constellation.wallet.KeyUtils

import scala.concurrent.ExecutionContextExecutor

object TestNode {

  def apply(seedHost: String = "")(
    implicit system: ActorSystem,
    materialize: ActorMaterializer,
    executionContext: ExecutionContextExecutor
  ): ConstellationNode = {
    val randomPort = scala.util.Random.nextInt(50000) + 5000
    val pubKey = KeyUtils.makeKeyPair().getPublic
    new ConstellationNode(pubKey, seedHost, "0.0.0.0", randomPort, actorNameSuffix = "ID_" + randomPort)
  }

}
