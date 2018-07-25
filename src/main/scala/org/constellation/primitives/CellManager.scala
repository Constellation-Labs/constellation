package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._

import scala.collection.mutable

//case class AddToCell(signedObservationEdge: SignedObservationEdge)

class CellManager(memPoolManager: ActorRef, metricsManager: ActorRef, peerManager: ActorRef) extends Actor {

  implicit val sheafOrdering: Ordering[EdgeSheaf] = Ordering.by{s: EdgeSheaf => s.score}

  implicit val cellOrdering: Ordering[CellKey] = Ordering.by{c => c.height -> c.depth}

  private val cells = mutable.SortedMap[CellKey, EdgeCell]()

  private val sheafs = mutable.SortedSet[EdgeSheaf]()

  def setCellSize(): Unit = metricsManager ! UpdateMetric("cellsActive", cells.size.toString)

  override def receive: Receive = {

    case InternalHeartbeat =>

      println("Cells " + cells)


    case g: GenesisObservation =>

      val soe = g.initialDistribution.signedObservationEdge
      val genesisHash = g.genesis.signedObservationEdge.hash
      val ck = CellKey(genesisHash, 1, 1)
      cells(ck) = EdgeCell(mutable.SortedSet(EdgeSheaf(soe, genesisHash, 1, 1, 10000D)))
      metricsManager ! IncrementMetric("cellsCreated")
      metricsManager ! UpdateMetric("genesisOEHash", genesisHash)
      setCellSize()

  }
}

