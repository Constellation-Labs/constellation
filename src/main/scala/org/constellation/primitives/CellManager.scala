package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._

import scala.collection.mutable

//case class AddToCell(signedObservationEdge: SignedObservationEdge)

class CellManager(memPoolManager: ActorRef, metricsManager: ActorRef) extends Actor {

  private val cells = mutable.HashMap[CellKey, EdgeCell]()

  def setCellSize(): Unit = metricsManager ! UpdateMetric("cellsActive", cells.size.toString)

  override def receive: Receive = {

    case InternalHeartbeat =>


    case g: GenesisObservation =>

      val soe = g.initialDistribution.signedObservationEdge
      val genesisHash = g.genesis.signedObservationEdge.hash
      val ck = CellKey(genesisHash, 1, 1)
      cells(ck) = EdgeCell(Seq(EdgeSheaf(soe, genesisHash, 1, 1, 10000D)))
      metricsManager ! IncrementMetric("cellsCreated")
      metricsManager ! UpdateMetric("genesisOEHash", genesisHash)
      setCellSize()

  }
}

