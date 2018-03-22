package org.constellation.utils

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
    val keyPair = KeyUtils.makeKeyPair()
    new ConstellationNode(keyPair, Some(Seq(seedHost)), "0.0.0.0", randomPort)
  }

}
