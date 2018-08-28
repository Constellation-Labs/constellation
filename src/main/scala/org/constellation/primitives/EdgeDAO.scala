package org.constellation.primitives

import akka.actor.ActorRef
import org.constellation.primitives.Schema._

import scala.collection.concurrent.TrieMap

trait EdgeDAO {


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var checkpointTips : Seq[SignedObservationEdge] = Seq()
  @volatile var validationTips : Seq[SignedObservationEdge] = Seq()

  val txMemPoolOE : TrieMap[String, Transaction] = TrieMap()
  val cpMemPoolOE : TrieMap[String, CheckpointEdge] = TrieMap()

  @volatile var txMemPoolOEThresholdMet: Set[String] = Set()
  @volatile var cpMemPoolOEThresholdMet: Set[String] = Set()

}
