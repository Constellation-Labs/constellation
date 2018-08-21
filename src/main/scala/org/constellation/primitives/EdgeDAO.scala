package org.constellation.primitives

import org.constellation.primitives.Schema.{GenesisObservation, TypedEdgeHash}

trait EdgeDAO {


  var genesisObservation: Option[GenesisObservation] = None

  @volatile var activeTips : Seq[TypedEdgeHash] = Seq()

}
