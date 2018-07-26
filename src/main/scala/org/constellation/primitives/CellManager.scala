package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._

//case class AddToCell(signedObservationEdge: SignedObservationEdge)

class CellManager(memPoolManager: ActorRef, metricsManager: ActorRef, peerManager: ActorRef) extends Actor {

  implicit val sheafOrdering: Ordering[EdgeSheaf] = Ordering.by{s: EdgeSheaf => s.score}

  implicit val cellOrdering: Ordering[CellKey] = Ordering.by{c => c.height -> c.depth}

  private val cells = mutable.SortedMap[CellKey, EdgeCell]()

  private val sheafs = mutable.SortedSet[EdgeSheaf]()

  def setCellSize(cellSize: Int): Unit = metricsManager ! UpdateMetric("cellsActive", cellSize.toString)

  override def receive = active(Map.empty)

  def active(cells: Map[CellKey, EdgeCell]): Receive = {

    case InternalHeartbeat =>

    case g: GenesisObservation =>

      val soe = g.initialDistribution.signedObservationEdge
      val genesisHash = g.genesis.signedObservationEdge.hash
      val ck = CellKey(genesisHash, 1, 1)
      val updatedCells = cells + (ck -> EdgeCell(mutable.SortedSet(EdgeSheaf(soe, genesisHash, 1, 1, 10000D))))
      metricsManager ! IncrementMetric("cellsCreated")
      metricsManager ! UpdateMetric("genesisOEHash", genesisHash)
      setCellSize(updatedCells.size)
      context become active(updatedCells)
  }
}

