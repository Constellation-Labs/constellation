package org.constellation.primitives

import akka.actor.ActorRef
import org.constellation.primitives.Schema.{GenesisObservation, Transaction, TypedEdgeHash}

import scala.collection.concurrent.TrieMap

trait EdgeDAO {

  var genesisObservation: Option[GenesisObservation] = None
  val minCheckpointFormationThreshold = 3

  @volatile var checkpointTips : Seq[TypedEdgeHash] = Seq()
  @volatile var validationTips : Seq[TypedEdgeHash] = Seq()

  val transactionMemPool : TrieMap[String, Transaction] = TrieMap()

  @volatile var transactionMemPoolThresholdMet: Set[String] = Set()

}
