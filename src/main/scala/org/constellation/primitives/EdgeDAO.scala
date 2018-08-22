package org.constellation.primitives

import org.constellation.primitives.Schema.{GenesisObservation, ResolvedTX, TypedEdgeHash}

trait EdgeDAO {


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var activeTips : Seq[TypedEdgeHash] = Seq()

  @volatile var memPoolOE : Seq[ResolvedTX] = Seq()

}
