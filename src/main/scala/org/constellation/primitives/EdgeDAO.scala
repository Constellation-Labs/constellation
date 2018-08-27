package org.constellation.primitives

import akka.actor.ActorRef
import org.constellation.primitives.Schema.{GenesisObservation, Transaction, TypedEdgeHash}

import scala.collection.concurrent.TrieMap

trait EdgeDAO {


  //var peerManager: ActorRef = _
  //var dbActor: ActorRef = _
  //var metricsManager: ActorRef = _


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var checkpointTips : Seq[TypedEdgeHash] = Seq()
  @volatile var validationTips : Seq[TypedEdgeHash] = Seq()

  val txMemPoolOE : TrieMap[String, Transaction] = TrieMap()

  @volatile var txMemPoolOEThresholdMet: Set[String] = Set()

}
