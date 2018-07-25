package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._

//case class AddToCell(signedObservationEdge: SignedObservationEdge)

class CellManager(memPoolManager: ActorRef, metricsManager: ActorRef, peerManager: ActorRef) extends Actor {


  def setCellSize(cellSize: Int): Unit = metricsManager ! UpdateMetric("cellsActive", cellSize.toString)

  override def receive = active(Map.empty)

  def active(cells: Map[CellKey, EdgeCell]): Receive = {

    case InternalHeartbeat =>

    case g: GenesisObservation =>

      val soe = g.initialDistribution.signedObservationEdge
      val genesisHash = g.genesis.signedObservationEdge.hash
      val ck = CellKey(genesisHash, 1, 1)
      val updatedCells = cells + (ck -> EdgeCell(Seq(EdgeSheaf(soe, genesisHash, 1, 1, 10000D))))
      metricsManager ! IncrementMetric("cellsCreated")
      metricsManager ! UpdateMetric("genesisOEHash", genesisHash)
      setCellSize(updatedCells.size)
      context become active(updatedCells)
  }
}

