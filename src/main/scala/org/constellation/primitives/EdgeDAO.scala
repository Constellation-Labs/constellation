package org.constellation.primitives

import org.constellation.primitives.Schema.{GenesisObservation, Transaction, TypedEdgeHash}

import scala.collection.concurrent.TrieMap

trait EdgeDAO {


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var activeTips : Seq[TypedEdgeHash] = Seq()

  val memPoolOE : TrieMap[String, Transaction] = TrieMap()

}
