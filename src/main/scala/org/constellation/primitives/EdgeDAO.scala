package org.constellation.primitives

import org.constellation.primitives.Schema.{GenesisObservation, Transaction, TypedEdgeHash}

import scala.collection.concurrent.TrieMap

trait EdgeDAO {


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var activeTips : Seq[TypedEdgeHash] = Seq()

  val txMemPoolOE : TrieMap[String, Transaction] = TrieMap()

  @volatile var txMemPoolOEThresholdMet: Set[String] = Set()

}
