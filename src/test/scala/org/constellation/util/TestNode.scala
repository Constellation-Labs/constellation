package org.constellation.util

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.implicits._
import org.constellation.crypto.KeyUtils
import org.constellation.domain.configuration.NodeConfig
import org.constellation.{ConstellationNode, ProcessingConfig}

import scala.concurrent.ExecutionContext
import scala.util.Try

object TestNode {

  private var nodes = Seq[ConstellationNode]()

  def apply(
    seedHosts: Seq[HostPort] = Seq(),
    keyPair: KeyPair = KeyUtils.makeKeyPair(),
    randomizePorts: Boolean = true,
    portOffset: Int = 0,
    isGenesisNode: Boolean = false,
    isLightNode: Boolean = false
  )(
    implicit system: ActorSystem,
    materializer: ActorMaterializer
//    executionContext: ExecutionContext
  ): ConstellationNode = {

    val randomPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9000 + portOffset
    val randomPeerPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9001 + portOffset

    val config = NodeConfig(
      seeds = seedHosts,
      primaryKeyPair = keyPair,
      metricIntervalSeconds = 10,
      isGenesisNode = isGenesisNode,
      isLightNode = isLightNode,
      httpPort = randomPort,
      peerHttpPort = randomPeerPort,
      attemptDownload = seedHosts.nonEmpty,
      allowLocalhostPeers = true,
      processingConfig = ProcessingConfig.testProcessingConfig
    )
    val node = new ConstellationNode(
      config
    )

    nodes = nodes :+ node
    node
  }

  def clearNodes(): Unit =
    Try {
      nodes.par.foreach(_.shutdown())
      nodes = Seq()
    }

}
