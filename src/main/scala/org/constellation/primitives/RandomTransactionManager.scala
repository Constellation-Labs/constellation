package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema.{Id, InternalHeartbeat, SendToAddress}
import akka.pattern.ask
import akka.util.Timeout
import constellation._

import scala.util.Random

class RandomTransactionManager(nodeManager: ActorRef, peerManager: ActorRef, metricsManager: ActorRef)(
                              implicit val timeout: Timeout
) extends Actor {

  val random = new Random()

  override def receive: Receive = {

    case InternalHeartbeat =>

      val peerIds = (peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
      def getRandomPeer = peerIds(random.nextInt(peerIds.size))
      nodeManager ! SendToAddress(getRandomPeer._1.address.address, random.nextInt(10000).toLong)
      metricsManager ! IncrementMetric("randomTransactionsGenerated")

  }
}
