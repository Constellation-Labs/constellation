package org.constellation.primitives

import akka.actor.ActorRef
import org.constellation.primitives.Schema._

import scala.collection.concurrent.TrieMap

trait EdgeDAO {


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var checkpointTips : Seq[SignedObservationEdge] = Seq()
  @volatile var validationTips : Seq[SignedObservationEdge] = Seq()

  val txMemPoolOE : TrieMap[String, Transaction] = TrieMap()
  val checkpointMemPool : TrieMap[String, CheckpointEdge] = TrieMap()
  val validationMemPool : TrieMap[String, ValidationEdge] = TrieMap()

  @volatile var txMemPoolOEThresholdMet: Set[String] = Set()
  @volatile var cpMemPoolOEThresholdMet: Set[String] = Set()
  @volatile var validationThresholdMet: Set[String] = Set()


  val resolveNotifierCallbacks: TrieMap[String, TrieMap[String, () => Unit]] = TrieMap()

}
