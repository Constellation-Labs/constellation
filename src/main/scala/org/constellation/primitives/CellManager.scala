package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._

import scala.collection.mutable

//case class AddToCell(signedObservationEdge: SignedObservationEdge)

class CellManager(memPoolManager: ActorRef, metricsManager: ActorRef) extends Actor {

  private val cells = mutable.HashMap[CellKey, EdgeCell]()

  override def receive: Receive = {

    case InternalHeartbeat =>

    case g: GenesisObservation =>

      g.initialDistribution.signedObservationEdge

  }
}

